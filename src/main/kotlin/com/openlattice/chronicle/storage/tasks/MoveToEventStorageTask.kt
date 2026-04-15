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
import com.openlattice.chronicle.storage.PostgresDataTables
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
                val allEntries = mutableListOf<UsageEventQueueEntry>()
                storageResolver.getPlatformStorage().connection.use { platform ->
                    platform.autoCommit = false
                    platform.createStatement().use { stmt ->
                        stmt.executeQuery(ChroniclePostgresTables.getMoveSql(128, UploadType.Android)).use { rs ->
                            while (rs.next()) {
                                val usageEventQueueEntries = ResultSetAdapters.usageEventQueueEntries(rs)
                                allEntries.addAll(usageEventQueueEntries.toEntryList())
                            }
                        }

                        if (allEntries.isNotEmpty()) {
                            logger.info("Total number of Android entries to move: ${allEntries.size}")
                            var redshiftSuccess = false
                            var postgresSuccess = false

                            // Write to Redshift (existing path, backward compat)
                            try {
                                val (flavor, hds) = storageResolver.getDefaultEventStorage()
                                if (flavor == PostgresFlavor.REDSHIFT || flavor == PostgresFlavor.ANY) {
                                    writeToRedshift(hds, allEntries)
                                    redshiftSuccess = true
                                }
                            } catch (ex: Exception) {
                                logger.error("Failed to write to Redshift event storage.", ex)
                            }

                            // Write to Postgres via upsert (new path)
                            try {
                                writeToPostgresUpsert(storageResolver.getPlatformStorage(), allEntries)
                                postgresSuccess = true
                            } catch (ex: Exception) {
                                logger.error("Failed to write to Postgres event storage.", ex)
                            }

                            if (!redshiftSuccess && !postgresSuccess) {
                                logger.error("Both Redshift and Postgres writes failed for ${allEntries.size} Android entries. Rolling back upload_buffer delete.")
                                platform.rollback()
                                platform.autoCommit = true
                                return
                            }

                            logger.info("Android write results: redshift={}, postgres={}", redshiftSuccess, postgresSuccess)
                        }
                    }
                    platform.commit()
                    logger.info("Committed upload_buffer delete for ${allEntries.size} Android entries.")
                    platform.autoCommit = true
                }
            } catch (ex: Exception) {
                logger.info("Unable to move data from aurora to event storage.", ex)
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

    /**
     * Writes usage events to Postgres using INSERT ... ON CONFLICT (dedup_hash_0, dedup_hash_1) upsert.
     * The dedup hashes are computed in SQL via hashtextextended with seeds 0 and 1, so no temp table
     * dedup cycle is needed.
     */
    private fun writeToPostgresUpsert(
        hds: HikariDataSource,
        data: List<UsageEventQueueEntry>,
    ): Int {
        if (data.isEmpty()) return 0

        return hds.connection.use { connection ->
            try {
                val studies = data.map { it.studyId.toString() }.toSet()
                val participants = data.map { it.participantId }.toSet()

                val insertBatchSize = min(data.size, RS_BATCH_SIZE)
                logger.info("Preparing Postgres upsert statement with batch size $insertBatchSize")
                val insertSql = PostgresDataTables.buildMultilineUpsertUsageEvents(insertBatchSize)

                val dr = data.size % RS_BATCH_SIZE
                val finalInsertSql = if (data.size > RS_BATCH_SIZE && dr != 0) {
                    logger.info("Preparing secondary Postgres upsert statement with batch size $dr")
                    PostgresDataTables.buildMultilineUpsertUsageEvents(dr)
                } else {
                    insertSql
                }

                val wc = data.chunked(RS_BATCH_SIZE).sumOf { subList ->
                    logger.info("Processing sublist of length ${subList.size} for Postgres upsert")
                    connection.prepareStatement(if (subList.size == insertBatchSize) insertSql else finalInsertSql)
                        .use { ps ->
                            StopWatch(
                                log = "Postgres upsert of ${subList.size} entries into ${RedshiftDataTables.CHRONICLE_USAGE_EVENTS.name} with studies = {} and participants = {}",
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
                                        val colIndex = indexBase + usageEventCol.colIndex
                                        val value = usageEventCol.value

                                        if (value == null) {
                                            ps.setObject(colIndex, null)
                                        } else {
                                            when (usageEventCol.datatype) {
                                                PostgresDatatype.TEXT -> ps.setString(colIndex, value as String)
                                                PostgresDatatype.TIMESTAMPTZ -> ps.setObject(colIndex, odtFromUsageEventColumn(value))
                                                PostgresDatatype.INTEGER -> ps.setInt(colIndex, value as Int)
                                                PostgresDatatype.BIGINT -> ps.setLong(colIndex, value as Long)
                                                else -> ps.setObject(colIndex, value)
                                            }
                                        }
                                    }
                                    ps.setObject(indexBase + UPLOAD_AT_INDEX, usageEventCols.uploadedAt)
                                    indexBase += RedshiftDataTables.CHRONICLE_USAGE_EVENTS.columns.size
                                }

                                val insertCount = ps.executeUpdate()
                                logger.info(
                                    "Postgres upserted $insertCount entities for ${RedshiftDataTables.CHRONICLE_USAGE_EVENTS.name} studies = {}, participantIds = {}",
                                    studies,
                                    participants
                                )
                                insertCount
                            }
                        }
                }

                return@use wc
            } catch (ex: Exception) {
                logger.error("Unable to upsert data to Postgres.", ex)
                throw ex
            }
        }
    }

    override fun getInitialDelay(): Long = PERIOD

    override fun getPeriod(): Long = PERIOD

    override fun getTimeUnit(): TimeUnit = TimeUnit.MILLISECONDS

    override fun getDependenciesClass(): Class<out MoveToEventStorageTaskDependencies> =
        MoveToEventStorageTaskDependencies::class.java


}