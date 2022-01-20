package com.openlattice.chronicle.services.upload

import com.geekbeast.configuration.postgres.PostgresFlavor
import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.util.StopWatch
import com.google.common.collect.ImmutableSet
import com.google.common.collect.Iterables
import com.google.common.collect.SetMultimap
import com.google.common.collect.Sets
import com.openlattice.chronicle.constants.EdmConstants.DATE_LOGGED_FQN
import com.openlattice.chronicle.constants.EdmConstants.DURATION_FQN
import com.openlattice.chronicle.constants.EdmConstants.END_DATE_TIME_FQN
import com.openlattice.chronicle.constants.EdmConstants.FULL_NAME_FQN
import com.openlattice.chronicle.constants.EdmConstants.RECORD_TYPE_FQN
import com.openlattice.chronicle.constants.EdmConstants.START_DATE_TIME_FQN
import com.openlattice.chronicle.constants.EdmConstants.STRING_ID_FQN
import com.openlattice.chronicle.constants.EdmConstants.TIMEZONE_FQN
import com.openlattice.chronicle.constants.EdmConstants.TITLE_FQN
import com.openlattice.chronicle.constants.EdmConstants.USER_FQN
import com.openlattice.chronicle.constants.OutputConstants
import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.services.ScheduledTasksManager
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.services.legacy.LegacyEdmResolver
import com.openlattice.chronicle.services.settings.OrganizationSettingsManager
import com.openlattice.chronicle.settings.AppUsageFrequency
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.APP_USAGE_INSERT_INDICES
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.getInsertIntoAppUsageTableSql
import com.openlattice.chronicle.storage.PostgresColumns.Companion.APP_USAGE_DATE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.APP_USAGE_TIMESTAMP
import com.openlattice.chronicle.storage.PostgresColumns.Companion.FQNS_TO_APP_USAGE_COLUMNS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.FQNS_TO_COLUMNS
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.CHRONICLE_USAGE_EVENTS
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.INSERT_USAGE_EVENT_COLUMN_INDICES
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.getAppendTembTableSql
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.getDeleteTempTableEntriesSql
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.getInsertIntoMergeUsageEventsTableSql
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.util.ChronicleServerUtil
import com.geekbeast.postgres.PostgresColumnDefinition
import com.geekbeast.postgres.PostgresDatatype
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.slf4j.event.Level
import java.security.InvalidParameterException
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.function.Consumer

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

class AppDataUploadService(
        private val storageResolver: StorageResolver,
        private val scheduledTasksManager: ScheduledTasksManager,
        private val enrollmentManager: EnrollmentManager,
        private val organizationSettingsManager: OrganizationSettingsManager
) : AppDataUploadManager {
    private val logger = LoggerFactory.getLogger(AppDataUploadService::class.java)

    private fun getTruncatedDateTimeHelper(dateTime: String?, chronoUnit: ChronoUnit): String? {
        return if (dateTime == null) {
            null
        } else OffsetDateTime.parse(dateTime)
                .truncatedTo(chronoUnit)
                .toString()
    }

    private fun getTruncatedDateTime(datetime: String, organizationId: UUID): String? {
        val settings = organizationSettingsManager.getOrganizationSettings(organizationId)
        //.getOrgAppSettings(AppComponent.CHRONICLE_DATA_COLLECTION, organizationId)
        val appUsageFreq = settings.chronicleDataCollection.appUsageFrequency
        val chronoUnit = if (appUsageFreq == AppUsageFrequency.HOURLY) ChronoUnit.HOURS else ChronoUnit.DAYS
        return getTruncatedDateTimeHelper(datetime, chronoUnit)
    }

    private fun getDateTimeValuesFromDeviceData(data: List<SetMultimap<UUID, Any>>): Set<OffsetDateTime> {
        val dateTimes: MutableSet<OffsetDateTime> = Sets.newHashSet()
        data.forEach(
                Consumer { entity: SetMultimap<UUID, Any> ->
                    // most date properties in the entity are of length 1
                    for (date in entity[LegacyEdmResolver.getPropertyTypeId(DATE_LOGGED_FQN)]) {
                        val parsedDateTime = OffsetDateTime
                                .parse(date.toString())

                        // filter out problematic entities with dates in the sixties
                        if (parsedDateTime.isAfter(OutputConstants.MINIMUM_DATE)) {
                            dateTimes.add(parsedDateTime)
                        }
                    }
                }
        )
        return dateTimes
    }

    private fun getFirstValueOrNull(entity: SetMultimap<UUID, Any>, fqn: FullQualifiedName): String? {
        val fqnId = LegacyEdmResolver.getPropertyTypeId(fqn)
        val value = Iterables.getFirst(entity[fqnId], null)
        return value?.toString()
    }

    private fun hasUserAppPackageName(organizationId: UUID?, packageName: String?): Boolean {
        return if (organizationId != null) {
            scheduledTasksManager.userAppsFullNamesByOrg.getOrDefault(packageName, ImmutableSet.of())
                    .contains(organizationId)
        } else scheduledTasksManager.userAppsFullNameValues.contains(packageName)
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
            organizationId: UUID,
            studyId: UUID,
            participantId: String,
            dataSourceId: String,
            data: List<SetMultimap<UUID, Any>>
    ): Int {
        StopWatch(
                log = "logging ${data.size} entries for ${ChronicleServerUtil.ORG_STUDY_PARTICIPANT_DATASOURCE}",
                level = Level.INFO,
                logger = logger,
                data.size,
                organizationId,
                studyId,
                participantId,
                dataSourceId
        ).use {
            try {
                val (flavor, hds) = storageResolver.resolve(studyId)

                val status = enrollmentManager.getParticipationStatus(organizationId, studyId, participantId)
                if (ParticipationStatus.NOT_ENROLLED == status) {
                    logger.warn(
                            "participant is not enrolled, ignoring upload" + ChronicleServerUtil.ORG_STUDY_PARTICIPANT_DATASOURCE,
                            organizationId,
                            studyId,
                            participantId,
                            dataSourceId
                    )
                    return 0
                }
                val isDeviceEnrolled = enrollmentManager.isKnownDatasource(
                        organizationId, studyId, participantId, dataSourceId
                )
                if (!isDeviceEnrolled) {
                    logger.error(
                            "data source not found, ignoring upload" + ChronicleServerUtil.ORG_STUDY_PARTICIPANT_DATASOURCE,
                            organizationId,
                            studyId,
                            participantId,
                            dataSourceId
                    )
                    return 0
                }

                logger.info(
                        "attempting to log data" + ChronicleServerUtil.ORG_STUDY_PARTICIPANT_DATASOURCE,
                        organizationId,
                        studyId,
                        participantId,
                        dataSourceId
                )

                val mappedUsageEventData = mapToStorageModel(data, FQNS_TO_COLUMNS, ::getUserEventsSequenceFilterPredicate, INSERT_USAGE_EVENT_COLUMN_INDICES, USAGE_EVENT_COLUMNS)
                val mappedAppUsageData = mapToStorageModel(data, FQNS_TO_APP_USAGE_COLUMNS, ::getAppUsageSequenceFilterPredicate, APP_USAGE_INSERT_INDICES, APP_USAGE_COLUMNS)

                StopWatch(log = "Writing ${data.size} entites to DB ")
                val written = when (flavor) {
                    PostgresFlavor.VANILLA -> writeToPostgres(hds, organizationId, studyId, participantId, mappedUsageEventData)
                    PostgresFlavor.REDSHIFT -> writeToRedshift(hds, organizationId, studyId, participantId, mappedUsageEventData)
                    else -> throw InvalidParameterException("Only regular postgres and redshift are supported.")
                }

                writeAppUsageDataToPostgres(hds, organizationId, studyId, participantId, mappedAppUsageData)

                //TODO: In reality we are likely to write less entities than were provided and are actually returning number processed so that client knows all is good
                if (data.size != written) {
                    //Should probably be an assertion as this should never happen.
                    logger.warn("Wrote $written entities, but expected to write ${data.size} entities")
                }
                return data.size
            } catch (exception: Exception) {
                logger.error(
                        "error logging data" + ChronicleServerUtil.ORG_STUDY_PARTICIPANT_DATASOURCE,
                        organizationId,
                        studyId,
                        participantId,
                        dataSourceId,
                        exception
                )
                return 0
            }
        }
    }

    private fun getUserEventsSequenceFilterPredicate(mappedCols: Map<String, UsageEventColumn>): Boolean {
        return true
    }

    private fun getAppUsageSequenceFilterPredicate(mappedCols: Map<String, UsageEventColumn>): Boolean {
        val appName = mappedCols[FQNS_TO_APP_USAGE_COLUMNS.getValue(FULL_NAME_FQN).name]?.value as String
        val eventDate = mappedCols[FQNS_TO_APP_USAGE_COLUMNS.getValue(DATE_LOGGED_FQN).name]?.value as String
        val dateLogged = OffsetDateTime.parse(eventDate)

        return !scheduledTasksManager.systemAppPackageNames.contains(appName) && dateLogged != null
    }

    private fun mapToStorageModel(
            data: List<SetMultimap<UUID, Any>>,
            mapper: Map<FullQualifiedName, PostgresColumnDefinition>,
            filterPredicate: (Map<String, UsageEventColumn>) -> Boolean,
            indices: Map<String, Int>,
            fqnColumns: List<FullQualifiedName>
    ): Sequence<Map<String, UsageEventColumn>> {
        return data.asSequence().map { usageEvent ->
            fqnColumns.associate { fqn ->
                val col = mapper.getValue(fqn)
                val colIndex = indices.getValue(col.name)
                val ptId = LegacyEdmResolver.getPropertyTypeId(fqn)
                val value = usageEvent[ptId]?.iterator()?.next()
                col.name to UsageEventColumn(col, colIndex, value)
            }
        }.filter { filterPredicate(it) }
    }

    private fun writeToRedshift(
            hds: HikariDataSource,
            organizationId: UUID,
            studyId: UUID,
            participantId: String,
            data: Sequence<Map<String, UsageEventColumn>>
    ): Int {
        return hds.connection.use { connection ->
            //Create the temporary merge table
            val tempMergeTable = CHRONICLE_USAGE_EVENTS.createTempTable()
            try {
                connection.autoCommit = false

                connection.createStatement().use { stmt -> stmt.execute(tempMergeTable.createTableQuery()) }

                val wc = connection
                        .prepareStatement(getInsertIntoMergeUsageEventsTableSql(tempMergeTable.name)).use { ps ->
                            //Should only need to set these once for prepared statement.
                            ps.setString(1, organizationId.toString())
                            ps.setString(2, studyId.toString())
                            ps.setString(3, participantId)

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

    private fun writeAppUsageDataToPostgres(
            hds: HikariDataSource,
            organizationId: UUID,
            studyId: UUID,
            participantId: String,
            data: Sequence<Map<String, UsageEventColumn>>
    ) {
        hds.connection.use { connection ->
            try {
                connection.autoCommit = false

                connection.prepareStatement(getInsertIntoAppUsageTableSql()).use { ps ->

                    ps.setObject(APP_USAGE_INSERT_INDICES.getValue(ORGANIZATION_ID.name), organizationId)
                    ps.setObject(APP_USAGE_INSERT_INDICES.getValue(STUDY_ID.name), studyId )
                    ps.setString(APP_USAGE_INSERT_INDICES.getValue(PARTICIPANT_ID.name), participantId)

                    data.forEach { mappedCols ->
                        mappedCols.values.forEach { usageEventColumn ->
                            val col = usageEventColumn.col
                            val index = usageEventColumn.colIndex
                            val value = usageEventColumn.value

                            // insert null if value wasn't specified
                            if (value == null) {
                                ps.setObject(index, null)
                            } else {
                                when (col.datatype) {
                                    PostgresDatatype.TEXT -> ps.setString(index, value as String)
                                    PostgresDatatype.TEXT_ARRAY -> {
                                        val valStr = value as String
                                        if (valStr.isBlank()) {
                                            ps.setArray(index, PostgresArrays.createUuidArray(connection, listOf()))
                                        } else {
                                            ps.setArray(index, PostgresArrays.createTextArray(connection, listOf(valStr)))
                                        }
                                    }
                                    PostgresDatatype.TIMESTAMPTZ -> ps.setObject(index, OffsetDateTime.parse(value as String))
                                    else -> ps.setObject(index, value)
                                }
                            }
                        }

                        // insert date
                        val dateCol = mappedCols.getValue(APP_USAGE_TIMESTAMP.name)
                        val date = OffsetDateTime.parse(dateCol.value as String).toLocalDate()
                        ps.setObject(APP_USAGE_INSERT_INDICES.getValue(APP_USAGE_DATE.name), date)

                        ps.addBatch()
                    }
                    ps.executeBatch().sum()
                }

                connection.commit()
                connection.autoCommit = true
                connection.close()
            } catch (ex: Exception) {
                logger.error("unable to write app usage data to postgres", ex)
                connection.rollback()
                connection.close()
                throw ex
            }
        }
    }

    private fun writeToPostgres(
            hds: HikariDataSource,
            organizationId: UUID,
            studyId: UUID,
            participantId: String,
            data: Sequence<Map<String, UsageEventColumn>>
    ): Int {
        return writeToRedshift(hds, organizationId, studyId, participantId, data)
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

private val APP_USAGE_COLUMNS = listOf(
        STRING_ID_FQN,
        FULL_NAME_FQN,
        DATE_LOGGED_FQN,
        USER_FQN,
        TITLE_FQN
)

// TODO: add DURATION_FQN, START_DATE_TIME_FQN, END_DATE_TIME_FQN etc to this list
private val USAGE_EVENT_COLUMNS = listOf(
        FULL_NAME_FQN,
        RECORD_TYPE_FQN,
        DATE_LOGGED_FQN,
        TIMEZONE_FQN,
        USER_FQN,
        TITLE_FQN
)

// TODO: remove this. not used anywhere
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
