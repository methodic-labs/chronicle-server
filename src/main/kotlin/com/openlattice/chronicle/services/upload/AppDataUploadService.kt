package com.openlattice.chronicle.services.upload

import com.geekbeast.configuration.postgres.PostgresFlavor
import com.geekbeast.postgres.PostgresColumnDefinition
import com.geekbeast.postgres.PostgresDatatype
import com.geekbeast.postgres.PostgresTableDefinition
import com.geekbeast.postgres.RedshiftTableDefinition
import com.geekbeast.util.StopWatch
import com.google.common.collect.SetMultimap
import com.openlattice.chronicle.android.ChronicleUsageEvent
import com.openlattice.chronicle.android.fromInteractionType
import com.openlattice.chronicle.constants.EdmConstants.*
import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.participants.ParticipantStats
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.services.legacy.LegacyEdmResolver
import com.openlattice.chronicle.services.studies.StudyManager
import com.openlattice.chronicle.storage.PostgresDataTables
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APPLICATION_LABEL
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_PACKAGE_NAME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.EVENT_TYPE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.FQNS_TO_COLUMNS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.INTERACTION_TYPE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMESTAMP
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMEZONE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.USERNAME
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
import java.time.LocalDate
import java.time.OffsetDateTime
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

class AppDataUploadService(
    private val storageResolver: StorageResolver,
    private val enrollmentManager: EnrollmentManager,
    private val studyManager: StudyManager,
) : AppDataUploadManager {
    private val logger = LoggerFactory.getLogger(AppDataUploadService::class.java)
    

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
    override fun uploadAndroidUsageEvents(
        studyId: UUID,
        participantId: String,
        sourceDeviceId: String,
        data: List<ChronicleUsageEvent>,
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
                val expectedSize = data.size
                doWrite(flavor, hds, studyId, participantId, mappedData, expectedSize)

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
        data: List<SetMultimap<UUID, Any>>,
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

                val mappedData = filter(mapLegacyDataToStorageModel(data))
                val expectedSize = data.size

                doWrite(flavor, hds, studyId, participantId, mappedData, expectedSize)

                return expectedSize
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

    private fun doWrite(
        flavor: PostgresFlavor,
        hds: HikariDataSource,
        studyId: UUID,
        participantId: String,
        mappedData: Sequence<Map<String, UsageEventColumn>>,
        expectedSize: Int,
    ): Int {
        val written = StopWatch(log = "Writing ${expectedSize} entites to DB ").use {
            when (flavor) {
                PostgresFlavor.VANILLA -> writeToPostgres(hds, studyId, participantId, mappedData)
                PostgresFlavor.REDSHIFT -> writeToRedshift(hds, studyId, participantId, mappedData)
                else -> throw InvalidParameterException("Only regular postgres and redshift are supported.")
            }
        }
        //TODO: In reality we are likely to write less entities than were provided and are actually returning number processed so that client knows all is good
        if (expectedSize != written) {
            //Should probably be an assertion as this should never happen.
            logger.warn("Wrote $written entities, but expected to write ${expectedSize} entities")
        }

        //Currently nothing is done with written, but here in case we need it in the future.
        return written
    }


    /**
     * This filters out events that have a null date logged and handles both String date time times from legacy events
     * and typed OffsetDateTime objects from non-legacy events.
     */
    private fun filter(mappedData: Sequence<Map<String, UsageEventColumn>>): Sequence<Map<String, UsageEventColumn>> {
        return mappedData.filter { mappedUsageEventCols ->
            val eventDate = mappedUsageEventCols[FQNS_TO_COLUMNS.getValue(DATE_LOGGED_FQN).name]?.value
            val dateLogged = odtFromUsageEventColumn(eventDate)
            dateLogged != null
        }
    }

    private fun <T> getUsageEventColumn(
        pcd: PostgresColumnDefinition,
        selector: () -> T,
    ): Pair<String, UsageEventColumn> {
        return pcd.name to UsageEventColumn(pcd, getInsertUsageEventColumnIndex(pcd), selector())
    }

    private fun mapToStorageModel(data: List<ChronicleUsageEvent>): Sequence<Map<String, UsageEventColumn>> {
        return data.asSequence().map { usageEvent ->
            mapOf(
//                getUsageEventColumn(STUDY_ID) { usageEvent.studyId },
//                getUsageEventColumn(PARTICIPANT_ID) { usageEvent.participantId },
                getUsageEventColumn(APP_PACKAGE_NAME) { usageEvent.appPackageName },
                getUsageEventColumn(INTERACTION_TYPE) { usageEvent.interactionType },
                getUsageEventColumn(EVENT_TYPE) { usageEvent.eventType },
                getUsageEventColumn(TIMESTAMP) { usageEvent.timestamp },
                getUsageEventColumn(TIMEZONE) { usageEvent.timezone },
                getUsageEventColumn(USERNAME) { usageEvent.user },
                getUsageEventColumn(APPLICATION_LABEL) { usageEvent.applicationLabel }
            )
        }
    }

    private fun mapLegacyDataToStorageModel(data: List<SetMultimap<UUID, Any>>): Sequence<Map<String, UsageEventColumn>> {
        return data.asSequence().map { usageEvent ->
            val usageEventCols = USAGE_EVENT_COLUMNS.associateTo(mutableMapOf()) { fqn ->
                val col = FQNS_TO_COLUMNS.getValue(fqn)
                val colIndex = getInsertUsageEventColumnIndex(col)
                val ptId = LegacyEdmResolver.getPropertyTypeId(fqn)
                val value = usageEvent[ptId]?.iterator()?.next()
                col.name to UsageEventColumn(col, colIndex, value)
            }

            //Compute event type column for legacy clients.
            val col = EVENT_TYPE
            val colIndex = getInsertUsageEventColumnIndex(col)
            val value = fromInteractionType((usageEventCols[INTERACTION_TYPE.name]?.value ?: "None") as String)
            usageEventCols[col.name] = UsageEventColumn(col, colIndex, value)
            usageEventCols
        }
    }

    private fun writeToRedshift(
        hds: HikariDataSource,
        studyId: UUID,
        participantId: String,
        data: Sequence<Map<String, UsageEventColumn>>,
        tempMergeTable: PostgresTableDefinition = CHRONICLE_USAGE_EVENTS.createTempTable(),
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

                                try {
                                    //Set insert value to null, if value was not provided.
                                    if (value == null) {
                                        ps.setObject(colIndex, null)
                                    } else {
                                        when (col.datatype) {
                                            PostgresDatatype.TEXT -> ps.setString(colIndex, value as String)
                                            PostgresDatatype.TIMESTAMPTZ -> ps.setObject(
                                                colIndex,
                                                odtFromUsageEventColumn(value)
                                            )
                                            PostgresDatatype.BIGINT -> ps.setLong(colIndex, value as Long)
                                            else -> ps.setObject(colIndex, value)
                                        }
                                    }
                                } catch (ex: Exception) {
                                    logger.info("Error writing $usageEventCol", ex)
                                    throw ex
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

                updateParticipantStats(data, studyId, participantId)

                connection.autoCommit = true
                return@use wc
            } catch (ex: Exception) {
                logger.error("Unable to save data to redshift.", ex)
                connection.rollback()
                throw ex
            }
        }
    }

    private fun odtFromUsageEventColumn(value: Any?): OffsetDateTime? {
        if (value == null) return null
        return when (value) {
            is String -> OffsetDateTime.parse(value)
            is OffsetDateTime -> value
            else -> throw UnsupportedOperationException("${value.javaClass.canonicalName} is not a supported date time class.")
        }
    }

    private fun updateParticipantStats(
        data: Sequence<Map<String, UsageEventColumn>>,
        studyId: UUID,
        participantId: String,
    ) {
        // unique dates
        val dates = data
            .mapNotNull { odtFromUsageEventColumn(it.getValue(TIMESTAMP.name).value) }
            .toMutableSet()

        val currentStats = studyManager.getParticipantStats(studyId, participantId)
        currentStats?.androidFirstDate?.let {
            dates += it
        }
        currentStats?.androidLastDate?.let {
            dates + it
        }
        val uniqueDates: MutableSet<LocalDate> = dates.map { it.toLocalDate() }.toMutableSet()
        currentStats?.androidUniqueDates?.let {
            uniqueDates += it
        }

        val minDate = dates.stream().min(OffsetDateTime::compareTo).get()
        val maxDate = dates.stream().max(OffsetDateTime::compareTo).get()

        val participantStats = ParticipantStats(
            studyId = studyId,
            participantId = participantId,
            androidUniqueDates = uniqueDates,
            androidFirstDate = minDate,
            androidLastDate = maxDate,
            tudFirstDate = currentStats?.tudFirstDate,
            tudLastDate = currentStats?.tudLastDate,
            tudUniqueDates = currentStats?.tudUniqueDates ?: setOf(),
            iosFirstDate = currentStats?.iosFirstDate,
            iosLastDate = currentStats?.iosLastDate,
            iosUniqueDates = currentStats?.iosUniqueDates ?: setOf()
        )
        studyManager.insertOrUpdateParticipantStats(participantStats)
    }

    private fun writeToPostgres(
        hds: HikariDataSource,
        studyId: UUID,
        participantId: String,
        data: Sequence<Map<String, UsageEventColumn>>,
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
    val value: Any?,
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
