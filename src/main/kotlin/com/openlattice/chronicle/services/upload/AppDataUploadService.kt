package com.openlattice.chronicle.services.upload

import com.geekbeast.configuration.postgres.PostgresFlavor
import com.geekbeast.postgres.PostgresColumnDefinition
import com.geekbeast.postgres.PostgresDatatype
import com.geekbeast.postgres.PostgresTableDefinition
import com.geekbeast.postgres.RedshiftTableDefinition
import com.geekbeast.util.StopWatch
import com.google.common.collect.SetMultimap
import com.openlattice.chronicle.constants.EdmConstants.*
import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.services.ScheduledTasksManager
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.services.legacy.LegacyEdmResolver
import com.openlattice.chronicle.storage.PostgresDataTables
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.FQNS_TO_COLUMNS
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.CHRONICLE_USAGE_EVENTS
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.getAppendTembTableSql
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.getDeleteTempTableEntriesSql
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.getInsertIntoMergeUsageEventsTableSql
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.getInsertUsageEventColumnIndex
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.util.ChronicleServerUtil
import com.zaxxer.hikari.HikariDataSource
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import org.slf4j.event.Level
import java.security.InvalidParameterException
import java.time.OffsetDateTime
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

class AppDataUploadService(
    private val storageResolver: StorageResolver,
    private val enrollmentManager: EnrollmentManager,
) : AppDataUploadManager {
    private val logger = LoggerFactory.getLogger(AppDataUploadService::class.java)

    private fun parseDateTime(dateTime: String?): OffsetDateTime? {
        if (dateTime == null) return null
        return try {
            OffsetDateTime.parse(dateTime)
        } catch (ex: Exception) {
            null
        }
    }

    /**
     * This routine implements once and only once append of client data.
     *
     * Assumptions:
     * - Client generates a UUID uniformly at random for each event and stores it in the id field.
     * - Client will retry upload until receives successful acknowledgement from the server.
     *
     * Data is first written into a postgres table which is periodically flushed to redshift for long term storage.
     *
     * The probability of the same UUID being generated twice for the same organization id/participant id/device
     * id/timestamp is unlikely to happen in the lifetime of our universe.
     */
    override fun upload(
        studyId: UUID,
        participantId: String,
        sourceDeviceId: String,
        data: List<SetMultimap<UUID, Any>>
    ): Int {
        StopWatch(
            log = "logging ${data.size} entries for ${ChronicleServerUtil.STUDY_PARTICIPANT_DATASOURCE}",
            level = Level.INFO,
            logger = logger,
            studyId,
            participantId,
            sourceDeviceId
        ).use {
            try {
                val (flavor, hds) = storageResolver.resolveAndGetFlavor(studyId)

                val status = enrollmentManager.getParticipationStatus(studyId, participantId)
                if (ParticipationStatus.NOT_ENROLLED == status) {
                    logger.warn(
                        "participant is not enrolled, ignoring upload" + ChronicleServerUtil.STUDY_PARTICIPANT_DATASOURCE,
                        studyId,
                        participantId,
                        sourceDeviceId
                    )
                    return 0
                }
                val deviceEnrolled = enrollmentManager.isKnownDatasource(studyId, participantId, sourceDeviceId)

                if (!deviceEnrolled) {
                    logger.error(
                        "data source not found, ignoring upload" + ChronicleServerUtil.STUDY_PARTICIPANT_DATASOURCE,
                        studyId,
                        participantId,
                        sourceDeviceId
                    )
                    return 0
                }

                logger.info(
                    "attempting to log data" + ChronicleServerUtil.STUDY_PARTICIPANT_DATASOURCE,
                    studyId,
                    participantId,
                    sourceDeviceId
                )

                val mappedData = filter(mapToStorageModel(data))

                StopWatch(log = "Writing ${data.size} entites to DB ")
                val written = when (flavor) {
                    PostgresFlavor.VANILLA -> writeToPostgres(hds, studyId, participantId, mappedData)
                    PostgresFlavor.REDSHIFT -> writeToRedshift(hds, studyId, participantId, mappedData)
                    else -> throw InvalidParameterException("Only regular postgres and redshift are supported.")
                }

                //TODO: In reality we are likely to write less entities than were provided and are actually returning number processed so that client knows all is good
                if (data.size != written) {
                    //Should probably be an assertion as this should never happen.
                    logger.warn("Wrote $written entities, but expected to write ${data.size} entities")
                }
                return data.size
            } catch (exception: Exception) {
                logger.error(
                    "error logging data" + ChronicleServerUtil.STUDY_PARTICIPANT_DATASOURCE,
                    studyId,
                    participantId,
                    sourceDeviceId,
                    exception
                )
                return 0
            }
        }
    }

    private fun filter(
        mappedData: Sequence<Map<String, UsageEventColumn>>
    ): Sequence<Map<String, UsageEventColumn>> {
        return mappedData.filter { mappedUsageEventCols ->
            val eventDate = mappedUsageEventCols[FQNS_TO_COLUMNS.getValue(DATE_LOGGED_FQN).name]?.value as String
            val dateLogged = parseDateTime(eventDate)
            dateLogged != null
        }
    }

    private fun mapToStorageModel(data: List<SetMultimap<UUID, Any>>): Sequence<Map<String, UsageEventColumn>> {
        return data.asSequence().map { usageEvent ->
            USAGE_EVENT_COLUMNS.associate { fqn ->
                val col = FQNS_TO_COLUMNS.getValue(fqn)
                val colIndex = getInsertUsageEventColumnIndex(col)
                val ptId = LegacyEdmResolver.getPropertyTypeId(fqn)
                val value = usageEvent[ptId]?.iterator()?.next()
                col.name to UsageEventColumn(col, colIndex, value)
            }
        }
    }

    private fun writeToRedshift(
        hds: HikariDataSource,
        studyId: UUID,
        participantId: String,
        data: Sequence<Map<String, UsageEventColumn>>,
        tempMergeTable: PostgresTableDefinition = CHRONICLE_USAGE_EVENTS.createTempTable()
    ): Int {
        return hds.connection.use { connection ->
            //Create the temporary merge table
            try {
                connection.autoCommit = false

                connection.createStatement().use { stmt -> stmt.execute(tempMergeTable.createTableQuery()) }

                val wc = connection
                    .prepareStatement(
                        getInsertIntoMergeUsageEventsTableSql(
                            tempMergeTable.name,
                            tempMergeTable !is RedshiftTableDefinition
                        )
                    )
                    .use { ps ->
                        //Should only need to set these once for prepared statement.
                        ps.setString(1, studyId.toString())
                        ps.setString(2, participantId)

                        data.forEach { usageEventCols ->
                            usageEventCols.values.forEach { usageEventCol ->

                                val col = usageEventCol.col
                                val colIndex = usageEventCol.colIndex
                                val value = usageEventCol.value

                                //Set insert value to null, if value was not provided.
                                if (value == null) {
                                    ps.setObject(colIndex, null)
                                } else {
                                    when (col.datatype) {
                                        PostgresDatatype.TEXT -> ps.setString(colIndex, value as String)
                                        PostgresDatatype.TIMESTAMPTZ -> ps.setObject(
                                            colIndex,
                                            OffsetDateTime.parse(value as String?)
                                        )
                                        PostgresDatatype.BIGINT -> ps.setLong(colIndex, value as Long)
                                        else -> ps.setObject(colIndex, value)
                                    }
                                }
                            }
                            ps.addBatch()
                        }
                        ps.executeBatch().sum()
                    }

                connection.createStatement().use { stmt ->
                    stmt.execute(getDeleteTempTableEntriesSql(tempMergeTable.name))
                    stmt.executeUpdate(getAppendTembTableSql(tempMergeTable.name))
                }

                connection.commit()
                connection.autoCommit = true
                return@use wc
            } catch (ex: Exception) {
                logger.error("Unable to save data to redshift.", ex)
                connection.rollback()
                throw ex
            }
        }
    }

    private fun writeToPostgres(
        hds: HikariDataSource,
        studyId: UUID,
        participantId: String,
        data: Sequence<Map<String, UsageEventColumn>>
    ): Int {
        return writeToRedshift(
            hds,
            studyId,
            participantId,
            data,
            PostgresDataTables.CHRONICLE_USAGE_EVENTS.createTempTableWithSuffix(
                RandomStringUtils.randomAlphanumeric(10)
            )
        )
    }


    companion object {
        private const val APP_USAGE_FREQUENCY = "appUsageFrequency"
    }
}


private data class UsageEventColumn(
    val col: PostgresColumnDefinition,
    val colIndex: Int,
    val value: Any?
)

private val USAGE_EVENT_COLUMNS = listOf(
    FULL_NAME_FQN,
    RECORD_TYPE_FQN,
    DATE_LOGGED_FQN,
    TIMEZONE_FQN,
    USER_FQN,
    TITLE_FQN
)

private val USAGE_STAT_COLUMNS = listOf(
    FULL_NAME_FQN,
    RECORD_TYPE_FQN,
    START_DATE_TIME_FQN,
    END_DATE_TIME_FQN,
    DURATION_FQN,
    DATE_LOGGED_FQN,
    TIMEZONE_FQN,
    TITLE_FQN,
)
