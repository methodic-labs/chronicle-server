package com.openlattice.chronicle.storage.tasks

import com.geekbeast.configuration.postgres.PostgresFlavor
import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.PostgresDatatype
import com.geekbeast.tasks.HazelcastFixedRateTask
import com.geekbeast.tasks.Task
import com.geekbeast.util.StopWatch
import com.google.common.util.concurrent.ListeningExecutorService
import com.google.common.util.concurrent.MoreExecutors
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.services.upload.UploadType
import com.openlattice.chronicle.services.upload.UsageEventQueueEntry
import com.openlattice.chronicle.storage.ChroniclePostgresTables
import com.openlattice.chronicle.storage.RedshiftColumns
import com.openlattice.chronicle.storage.RedshiftDataTables
import com.openlattice.chronicle.storage.odtFromUsageEventColumn
import com.zaxxer.hikari.HikariDataSource
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import org.slf4j.event.Level
import java.security.InvalidParameterException
import java.time.OffsetDateTime
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import kotlin.math.min

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class MoveToEventStorageTask : HazelcastFixedRateTask<MoveToEventStorageTaskDependencies> {
    companion object {
        private const val RS_BATCH_SIZE = 3276
        private const val PERIOD = 5*60000L
        private val UPLOAD_AT_INDEX = RedshiftDataTables.getInsertUsageEventColumnIndex(RedshiftColumns.UPLOADED_AT)
        private val logger = LoggerFactory.getLogger(MoveToEventStorageTask::class.java)

        private val executor: ListeningExecutorService =
            MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(3))
    }

    override fun runTask() {
        val f = executor.submit {
            moveToEventStorage()
        }
        try {
            f.get(1, TimeUnit.HOURS)
        } catch (timeoutException: TimeoutException) {
            logger.error("Timed out after one hour when moving events to event storage.", timeoutException)
            f.cancel(true)
        } catch (ex: Exception) {
            logger.error("Exception when moving events to event storage.", ex)
        }
    }

    override fun getName(): String = Task.MOVE_TO_EVENT_STORAGE.name

    private fun moveToEventStorage() {
        with(getDependency()) {
            try {
                logger.info("Moving data from aurora to event storage.")
                val queueEntriesByFlavor: MutableMap<PostgresFlavor, MutableList<UsageEventQueueEntry>> = mutableMapOf()
                storageResolver.getPlatformStorage().connection.use { platform ->
                    platform.autoCommit = false
                    platform.createStatement().use { stmt ->
                        stmt.executeQuery(ChroniclePostgresTables.getMoveSql(128, UploadType.Android)).use { rs ->
                            while (rs.next()) {
                                val usageEventQueueEntries = ResultSetAdapters.usageEventQueueEntries(rs)
                                val (flavor, _) = storageResolver.resolveAndGetFlavor(usageEventQueueEntries.studyId)
                                queueEntriesByFlavor.getOrPut(flavor) { mutableListOf() }
                                    .addAll(usageEventQueueEntries.toEntryList())
                            }
                        }
                        logger.info("Total number of entries for redshift: ${(queueEntriesByFlavor[PostgresFlavor.REDSHIFT] ?: listOf()).size}")
                        logger.info("Total number of entries for postgres: ${(queueEntriesByFlavor[PostgresFlavor.VANILLA] ?: listOf()).size}")
                        queueEntriesByFlavor.forEach { (postgresFlavor, usageEventQueueEntries) ->
                            if (usageEventQueueEntries.isEmpty()) return@forEach
                            when (postgresFlavor) {
                                PostgresFlavor.REDSHIFT -> writeToRedshift(
                                    storageResolver.getEventStorageWithFlavor(PostgresFlavor.REDSHIFT),
                                    usageEventQueueEntries
                                )
                                PostgresFlavor.VANILLA -> writeToPostgres(
                                    storageResolver.getEventStorageWithFlavor(PostgresFlavor.VANILLA),
                                    usageEventQueueEntries
                                )
                                else -> throw InvalidParameterException("Invalid postgres flavor: ${postgresFlavor.name}")
                            }
                        }
                    }
                    platform.commit()
                    platform.autoCommit = true
                }
                logger.info("Successfully moved data to event storage.")
            } catch (ex: Exception) {
                logger.info("Unable to move data from aurora to redshift.", ex)
                throw ex
            }
        }
    }

    private fun writeToRedshift(
        hds: HikariDataSource,
        data: List<UsageEventQueueEntry>,
        includeOnConflict: Boolean = false,
    ): Int {
        if (data.isEmpty()) return 0

        return hds.connection.use { connection ->
            //Create the temporary merge table
            try {
                //TODO: May be based this off data being inserted instead?
                var minEventTimestamp: OffsetDateTime = OffsetDateTime.MAX
                var maxEventTimestamp: OffsetDateTime = OffsetDateTime.MIN

                val studies = data.map { it.studyId.toString() }.toSet()
                val participants = data.map { it.participantId }.toSet()

                // There are two prepared statements one for the data array from 0 up to RS_BATCH_SIZE elements.
                // After RS_BATCH_SIZE elements the insert prepared statement covers all the chunks except the last chunk of RS_BATCH_SIZE elements
                // finalInsert won't be used subList.size is never unequal to the insertBatchSize (shoudl only happen for data.size > RS_BATCH_SIZE and data.size % RS_BATCH_SIZE != 0

                val insertBatchSize = min(data.size, RS_BATCH_SIZE)
                logger.info("Preparing primary insert statement with batch size $insertBatchSize")
                val insertSql = RedshiftDataTables.buildMultilineInsertUsageEvents(
                    insertBatchSize,
                    includeOnConflict
                )

                val dr = data.size % RS_BATCH_SIZE

                val finalInsertSql = if (data.size > RS_BATCH_SIZE && dr != 0) {
                    logger.info("Preparing secondary insert statement with batch size $dr")
                    RedshiftDataTables.buildMultilineInsertUsageEvents(
                        dr,
                        includeOnConflict
                    )
                } else {
                    insertSql
                }

                val wc = data.chunked(RS_BATCH_SIZE).sumOf { subList ->
                    logger.info("Processing sublist of length ${subList.size}")
                    connection.prepareStatement(if (subList.size == insertBatchSize) insertSql else finalInsertSql)
                        .use { ps ->

                            //Should only need to set these once for prepared statement.
                            StopWatch(
                                log = "Inserting ${data.size} entries into ${RedshiftDataTables.CHRONICLE_USAGE_EVENTS.name} with studies = {} and participants = {}",
                                level = Level.INFO,
                                logger = logger,
                                studies,
                                participants
                            ).use {
                                var indexBase = 0
                                subList.forEach { usageEventCols ->
                                    ps.setString(indexBase + 1, usageEventCols.studyId.toString())
                                    ps.setString(indexBase + 2, usageEventCols.participantId)
                                    usageEventCols.data.values.forEach { usageEventCol ->
                                        //TODO: If we ever change the columns, we need to do a lookup for colIndex by name every time.
                                        val colIndex = indexBase + usageEventCol.colIndex
                                        val value = usageEventCol.value

                                        try {
                                            //Set insert value to null, if value was not provided.
                                            if (value == null) {
                                                ps.setObject(colIndex, null)
                                            } else {
                                                when (usageEventCol.datatype) {
                                                    PostgresDatatype.TEXT -> ps.setString(colIndex, value as String)
                                                    PostgresDatatype.TIMESTAMPTZ -> {
                                                        val odt = odtFromUsageEventColumn(value)
                                                        ps.setObject(
                                                            colIndex,
                                                            odt
                                                        )
                                                        //We need to keep track the min and max event timestamps for this batch
                                                        if (odt != null && usageEventCol.name == RedshiftColumns.TIMESTAMP.name) {
                                                            if (odt.isBefore(minEventTimestamp)) {
                                                                minEventTimestamp = odt
                                                            }
                                                            if (odt.isAfter(maxEventTimestamp)) {
                                                                maxEventTimestamp = odt
                                                            }
                                                        }
                                                    }
                                                    PostgresDatatype.INTEGER -> ps.setInt(colIndex, value as Int)
                                                    PostgresDatatype.BIGINT -> ps.setLong(colIndex, value as Long)
                                                    else -> ps.setObject(colIndex, value)
                                                }
                                            }
                                        } catch (ex: Exception) {
                                            logger.info("Error writing $usageEventCol", ex)
                                            throw ex
                                        }
                                    }
                                    ps.setObject(indexBase + UPLOAD_AT_INDEX, usageEventCols.uploadedAt)
                                    indexBase += RedshiftDataTables.CHRONICLE_USAGE_EVENTS.columns.size
//                                    logger.info(
//                                        "Added batch for ${ChronicleServerUtil.STUDY_PARTICIPANT}",
//                                        usageEventCols.studyId,
//                                        usageEventCols.participantId
//                                    )

                                }

                                StopWatch(
                                    log = "Executing update on ${subList.size} entries into ${RedshiftDataTables.CHRONICLE_USAGE_EVENTS.name} with studies = {} and participants = {}",
                                    level = Level.INFO,
                                    logger = logger,
                                    studies,
                                    participants
                                ).use {
                                    val insertCount = ps.executeUpdate()
                                    logger.info(
                                        "Inserted $insertCount entities for ${RedshiftDataTables.CHRONICLE_USAGE_EVENTS.name} studies = {}, participantIds = {}",
                                        studies,
                                        participants
                                    )
                                    insertCount
                                }
                            }

                        }
                }


//                StopWatch(
//                    log = "Merging entries for $tempInsertTableName with studies = {} and participants = {}",
//                    level = Level.INFO,
//                    logger = logger,
//                    studies,
//                    participants
//                ).use {
//                    connection.createStatement().use { stmt ->
//                        stmt.execute(getAppendTempTableSql(tempInsertTableName));
//                        stmt.execute("DROP TABLE $tempInsertTableName")
//                    }
//                }
//
                val tempTableName = "duplicate_events_${RandomStringUtils.randomAlphanumeric(10)}"


                //Create a table that contains any duplicate values introduced by this latest upload for the minimum upload_at value
                StopWatch(
                    log = "Creating duplicates table for studies = {} and participants = {} ",
                    level = Level.INFO,
                    logger = logger,
                    studies,
                    participants
                ).use {
                    connection.createStatement()
                        .use { stmt -> stmt.execute(RedshiftDataTables.createTempTableOfDuplicates(tempTableName)) }
                    connection.prepareStatement(RedshiftDataTables.buildTempTableOfDuplicates(tempTableName))
                        .use { ps ->
                            ps.setArray(1, PostgresArrays.createTextArray(connection, studies))
                            ps.setArray(2, PostgresArrays.createTextArray(connection, participants))
                            ps.setObject(3, minEventTimestamp)
                            ps.setObject(4, maxEventTimestamp)
                            ps.execute()
                        }
                }

                //Delete the duplicates, if any from chronicle_usage_events and drop the temporary table.
                StopWatch(
                    log = "Deleting duplicates for studies = {} and participants = {} ",
                    level = Level.INFO,
                    logger = logger,
                    studies,
                    participants
                ).use {
                    connection.createStatement().use { stmt ->
                        stmt.execute(RedshiftDataTables.getDeleteUsageEventsFromTempTable(tempTableName))
                        stmt.execute("DROP TABLE $tempTableName")
                    }
                }

                return@use wc
            } catch (ex: Exception) {
                logger.error("Unable to save data to redshift.", ex)
                throw ex
            }
        }
    }

    private fun writeToPostgres(
        hds: HikariDataSource,
        data: List<UsageEventQueueEntry>,
    ): Int {
        return writeToRedshift(
            hds,
            data
        )
    }

    override fun getInitialDelay(): Long = PERIOD

    override fun getPeriod(): Long = PERIOD

    override fun getTimeUnit(): TimeUnit = TimeUnit.MILLISECONDS

    override fun getDependenciesClass(): Class<out MoveToEventStorageTaskDependencies> =
        MoveToEventStorageTaskDependencies::class.java


}