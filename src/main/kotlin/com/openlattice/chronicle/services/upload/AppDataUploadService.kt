package com.openlattice.chronicle.services.upload

import com.geekbeast.configuration.postgres.PostgresFlavor
import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.PostgresColumnDefinition
import com.geekbeast.postgres.PostgresDatatype
import com.geekbeast.util.StopWatch
import com.google.common.collect.SetMultimap
import com.google.common.util.concurrent.ListeningExecutorService
import com.google.common.util.concurrent.MoreExecutors
import com.openlattice.chronicle.android.ChronicleUsageEvent
import com.openlattice.chronicle.android.fromInteractionType
import com.openlattice.chronicle.constants.EdmConstants.*
import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.participants.ParticipantStats
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.services.legacy.LegacyEdmResolver
import com.openlattice.chronicle.services.studies.StudyManager
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.UPLOAD_BUFFER
import com.openlattice.chronicle.storage.PostgresColumns
import com.openlattice.chronicle.storage.PostgresColumns.Companion.USAGE_EVENTS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APPLICATION_LABEL
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_PACKAGE_NAME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.EVENT_TYPE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.FQNS_TO_COLUMNS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.INTERACTION_TYPE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMESTAMP
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMEZONE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.UPLOADED_AT
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.USERNAME
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.CHRONICLE_USAGE_EVENTS
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.buildTempTableOfDuplicates
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.createTempTableOfDuplicates
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.getAppendTempTableSql
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.getDeleteUsageEventsFromTempTable
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.getInsertIntoUsageEventsTableSql
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.getInsertUsageEventColumnIndex
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.util.ChronicleServerUtil
import com.zaxxer.hikari.HikariDataSource
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import org.slf4j.event.Level
import java.time.LocalDate
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.Executors

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

class AppDataUploadService(
    private val storageResolver: StorageResolver,
    private val enrollmentManager: EnrollmentManager,
    private val studyManager: StudyManager,
) : AppDataUploadManager {
    companion object {
        private val logger = LoggerFactory.getLogger(AppDataUploadService::class.java)
        private val UPLOAD_AT_INDEX = getInsertUsageEventColumnIndex(UPLOADED_AT)
        private val mapper = ObjectMappers.getJsonMapper()

        private val executor: ListeningExecutorService =
            MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1))

        private val INSERT_USAGE_EVENTS_SQL = """
                    INSERT INTO ${UPLOAD_BUFFER.name} (${STUDY_ID.name},${PARTICIPANT_ID.name},${USAGE_EVENTS.name}, ${PostgresColumns.UPLOADED_AT.name}) VALUES (?,?,?,?)
                    ON CONFLICT (${STUDY_ID.name}, ${PARTICIPANT_ID.name}) 
                    DO UPDATE SET ${USAGE_EVENTS.name} = ${UPLOAD_BUFFER.name}.${USAGE_EVENTS.name} || EXCLUDED.${USAGE_EVENTS.name}
                """.trimIndent()

        private fun getMoveSql(batchSize: Int = 65536) = """
                DELETE FROM ${UPLOAD_BUFFER.name} WHERE (${STUDY_ID.name}, ${PARTICIPANT_ID.name}) IN (
                    SELECT ${STUDY_ID.name},${PARTICIPANT_ID.name},${USAGE_EVENTS.name} 
                    FROM ${UPLOAD_BUFFER.name}
                    ORDER BY ${STUDY_ID.name},${PARTICIPANT_ID.name}
                    FOR UPDATE SKIP LOCKED
                    LIMIT $batchSize
                    )
                RETURNING *
                """.trimIndent()
    }

    init {
        executor.run {
            while (true) {
                moveToEventStorage()
                Thread.sleep(5 * 60 * 1000)
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
    override fun uploadAndroidUsageEvents(
        studyId: UUID,
        participantId: String,
        sourceDeviceId: String,
        data: List<ChronicleUsageEvent>,
        uploadedAt: OffsetDateTime,
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
                doWrite(studyId, participantId, mappedData, expectedSize, uploadedAt)

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
        uploadedAt: OffsetDateTime,
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

                doWrite(studyId, participantId, mappedData, expectedSize, uploadedAt)

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
        studyId: UUID,
        participantId: String,
        mappedData: Sequence<Map<String, UsageEventColumn>>,
        expectedSize: Int,
        uploadedAt: OffsetDateTime,
    ): Int {
        val dataList = mappedData.toList()
        val written = StopWatch(
            log = "Writing ${dataList.size} entites (expected: $expectedSize) to Aurora for studyId = $studyId, participantId = $participantId ",
            level = Level.INFO,
            logger = logger,
        ).use {
            storageResolver.getPlatformStorage().connection.use { connection ->

                connection.prepareStatement(INSERT_USAGE_EVENTS_SQL).use { ps ->
                    ps.setObject(1, studyId)
                    ps.setString(2, participantId)
                    ps.setString(3, mapper.writeValueAsString(dataList))
                    ps.setObject(4, uploadedAt)
                    ps.executeUpdate()
                }
            }
        }
//        val written = StopWatch(log = "Writing ${expectedSize} entites to DB ").use {
//            when (flavor) {
//                PostgresFlavor.VANILLA -> writeToPostgres(hds, studyId, participantId, mappedData, uploadedAt)
//                PostgresFlavor.REDSHIFT -> writeToRedshift(hds, studyId, participantId, mappedData, uploadedAt)
//                else -> throw InvalidParameterException("Only regular postgres and redshift are supported.")
//            }
//        }
        //TODO: In reality we are likely to write less entities than were provided and are actually returning number processed so that client knows all is good
        if (expectedSize != written) {
            //Should probably be an assertion as this should never happen.
            logger.warn("Wrote $written entities, but expected to write $expectedSize entities")
        }


        //Currently nothing is done with written, but here in case we need it in the future.
        return written
    }

    override fun moveToEventStorage() {
        logger.info("Moving data from aurora to redshift.")
        val queueEntriesByFlavor: MutableMap<PostgresFlavor, MutableList<UsageEventQueueEntry>> = mutableMapOf()
        try {
            storageResolver.getPlatformStorage().connection.use { platform ->
                platform.autoCommit = false
                platform.createStatement().use { stmt ->
                    stmt.executeQuery(getMoveSql()).use { rs ->
                        while (rs.next()) {
                            val usageEventQueueEntry: UsageEventQueueEntry = ResultSetAdapters.usageEventQueueEntry(rs)
                            val (flavor, _) = storageResolver.resolveAndGetFlavor(usageEventQueueEntry.studyId)
                            queueEntriesByFlavor.getOrPut(flavor) { mutableListOf() }.add(usageEventQueueEntry)
                        }
                    }
                    writeToRedshift(
                        storageResolver.getEventStorageWithFlavor(PostgresFlavor.REDSHIFT),
                        queueEntriesByFlavor.getOrPut(PostgresFlavor.REDSHIFT) { mutableListOf() }
                    )
                    writeToPostgres(
                        storageResolver.getEventStorageWithFlavor(PostgresFlavor.VANILLA),
                        queueEntriesByFlavor.getOrPut(PostgresFlavor.REDSHIFT) { mutableListOf() }
                    )
                }
                platform.commit()
                platform.autoCommit = true
            }
        } catch (ex: Exception) {
            logger.info("Unable to move data from aurora to redshift.", ex)
            throw ex
        }
    }

    /**
     * This filters out events that have a null date logged and handles both String date time times from legacy events
     * and typed OffsetDateTime objects from non-legacy events.
     */
    private fun filter(mappedData: Sequence<Map<String, UsageEventColumn>>): Sequence<Map<String, UsageEventColumn>> {
        return mappedData.filter { mappedUsageEventCols ->
            val eventDate = mappedUsageEventCols[FQNS_TO_COLUMNS.getValue(DATE_LOGGED_FQN).name]?.value
            val dateLogged = odtFromUsageEventColumn(eventDate)

            val appPackageName = checkNotNull(mappedUsageEventCols[APP_PACKAGE_NAME.name]?.value as String?) {
                "Application package name cannot be null."
            }

            dateLogged != null && !appPackageName.contains("[")
        }
    }

    private fun <T> getUsageEventColumn(
        pcd: PostgresColumnDefinition,
        selector: () -> T,
    ): Pair<String, UsageEventColumn> {
        return pcd.name to UsageEventColumn(pcd.name, pcd.datatype, getInsertUsageEventColumnIndex(pcd), selector())
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
                col.name to UsageEventColumn(col.name, col.datatype, colIndex, value)
            }

            //Compute event type column for legacy clients.
            val col = EVENT_TYPE
            val colIndex = getInsertUsageEventColumnIndex(col)
            val value = fromInteractionType((usageEventCols[INTERACTION_TYPE.name]?.value ?: "None") as String)
            usageEventCols[col.name] = UsageEventColumn(col.name, col.datatype, colIndex, value)
            usageEventCols
        }
    }

    private fun writeToRedshift(
        hds: HikariDataSource,
        data: List<UsageEventQueueEntry>,
        includeOnConflict: Boolean = false,
    ): Int {
        if( data.isEmpty() ) return 0
        return hds.connection.use { connection ->
            //Create the temporary merge table
            try {
                var minEventTimestamp: OffsetDateTime = OffsetDateTime.MAX
                var maxEventTimestamp: OffsetDateTime = OffsetDateTime.MIN
                val tempInsertTableName = "staging_events_${RandomStringUtils.randomAlphanumeric(10)}"
                val studies = data.map { it.studyId.toString() }
                val participants = data.map { it.participantId }

                connection.createStatement().use { stmt ->
                    stmt.execute("CREATE TEMPORARY TABLE $tempInsertTableName (LIKE ${CHRONICLE_USAGE_EVENTS.name})")
                }

                logger.info(
                    "Created temporary table $tempInsertTableName for upload with studies = {} and participants = {}",
                    studies,
                    participants
                )

                connection.autoCommit = false
                val wc = connection
                    .prepareStatement(getInsertIntoUsageEventsTableSql(tempInsertTableName, includeOnConflict))
                    .use { ps ->
                        //Should only need to set these once for prepared statement.

                        StopWatch(
                            log = "Inserting entries for $tempInsertTableName with studies = {} and participants = {}",
                            level = Level.INFO,
                            logger = logger,
                            studies,
                            participants
                        ).use {
                            data.forEach { usageEventCols ->
                                ps.setString(1, usageEventCols.studyId.toString())
                                ps.setString(2, usageEventCols.participantId)
                                usageEventCols.data.values.forEach { usageEventCol ->
                                    //TODO: If we ever change the columns, we need to do a lookup for colIndex by name every time.
                                    val colIndex = usageEventCol.colIndex
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
                                                    if (odt != null && usageEventCol.name == TIMESTAMP.name) {
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
                                ps.setObject(UPLOAD_AT_INDEX, usageEventCols.uploadedAt)
//                              logger.info("Added batch for ${ChronicleServerUtil.STUDY_PARTICIPANT}", studyId, participantId)
                                ps.addBatch()
                            }
                            val insertCount = ps.executeBatch().sum()
                            connection.commit()
                            logger.info(
                                "Inserted $insertCount entities for $tempInsertTableName studies = {}, participantIds = {}",
                                studies,
                                participants
                            )
                            connection.autoCommit = true
                            insertCount
                        }
                    }

                StopWatch(
                    log = "Merging entries for $tempInsertTableName with studies = {} and participants = {}",
                    level = Level.INFO,
                    logger = logger,
                    studies,
                    participants
                ).use {
                    connection.createStatement().use { stmt ->
                        stmt.execute(getAppendTempTableSql(tempInsertTableName));
                        stmt.execute("DROP TABLE $tempInsertTableName")
                    }
                }

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
                        .use { stmt -> stmt.execute(createTempTableOfDuplicates(tempTableName)) }
                    connection.prepareStatement(buildTempTableOfDuplicates(tempTableName)).use { ps ->
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
                        stmt.execute(getDeleteUsageEventsFromTempTable(tempTableName))
                        stmt.execute("DROP TABLE $tempTableName")
                    }
                }

                //TODO: Make this more efficient
                data.groupBy { it.studyId to it.participantId }.forEach { (key, qe) ->
                    val (studyId, participantId) = key
                    updateParticipantStats(qe.map { it.data }, studyId, participantId)
                }

                return@use wc
            } catch (ex: Exception) {
                logger.error("Unable to save data to redshift.", ex)
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
        data: List<Map<String, UsageEventColumn>>,
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
            androidLastPing = OffsetDateTime.now(),
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
        data: List<UsageEventQueueEntry>,
    ): Int {
        return writeToRedshift(
            hds,
            data
        )
    }
}


data class UsageEventQueueEntry(
    val studyId: UUID,
    val participantId: String,
    val data: Map<String, UsageEventColumn>,
    val uploadedAt: OffsetDateTime,
)

data class UsageEventColumn(
    val name: String,
    val datatype: PostgresDatatype,
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
