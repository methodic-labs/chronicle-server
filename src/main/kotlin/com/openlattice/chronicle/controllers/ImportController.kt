package com.openlattice.chronicle.controllers

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.geekbeast.jdbc.DataSourceManager
import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.postgres.PostgresDatatype
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.geekbeast.util.log
import com.google.common.collect.Maps
import com.google.common.util.concurrent.MoreExecutors
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicates
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.*
import com.openlattice.chronicle.authorization.mapstores.UserMapstore
import com.openlattice.chronicle.candidates.Candidate
import com.openlattice.chronicle.constants.OutputConstants
import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.import.ImportApi
import com.openlattice.chronicle.import.ImportApi.Companion.APP_USAGE_SURVEY
import com.openlattice.chronicle.import.ImportApi.Companion.CONTROLLER
import com.openlattice.chronicle.import.ImportApi.Companion.PARTICIPANT_STATS
import com.openlattice.chronicle.import.ImportApi.Companion.STUDIES
import com.openlattice.chronicle.import.ImportApi.Companion.SYSTEM_APPS
import com.openlattice.chronicle.import.ImportApi.Companion.TIME_USE_DIARY
import com.openlattice.chronicle.import.ImportStudiesConfiguration
import com.openlattice.chronicle.participants.Participant
import com.openlattice.chronicle.participants.ParticipantStats
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.services.candidates.CandidateService
import com.openlattice.chronicle.services.studies.StudyService
import com.openlattice.chronicle.services.surveys.SurveysService
import com.openlattice.chronicle.services.timeusediary.TimeUseDiaryService
import com.openlattice.chronicle.services.upload.AppDataUploadService
import com.openlattice.chronicle.storage.ChroniclePostgresTables
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.LEGACY_STUDY_IDS
import com.openlattice.chronicle.storage.PostgresColumns
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SETTINGS
import com.openlattice.chronicle.storage.RedshiftColumns
import com.openlattice.chronicle.study.Study
import com.openlattice.chronicle.timeusediary.TimeUseDiaryResponse
import com.zaxxer.hikari.HikariDataSource
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.sql.Array
import java.sql.ResultSet
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZoneId
import java.util.*
import java.util.concurrent.Executors

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@RestController
@RequestMapping(CONTROLLER)
class ImportController(
    private val studyService: StudyService,
    private val candidateService: CandidateService,
    private val timeUseDiaryService: TimeUseDiaryService,
    private val appDataUploadService: AppDataUploadService,
    private val idGenerationService: HazelcastIdGenerationService,
    private val dataSourceManager: DataSourceManager,
    override val authorizationManager: AuthorizationManager,
    override val auditingManager: AuditingManager,
    hazelcast: HazelcastInstance
) : ImportApi, AuthorizingComponent {

    companion object {
        private val logger = LoggerFactory.getLogger(ImportController::class.java)
        private val mapper: ObjectMapper = ObjectMappers.newJsonMapper()
        private val executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(8))

        private val INSERT_LEGACY_STUDY_ID_SQL = """
            INSERT INTO ${LEGACY_STUDY_IDS.name}(${PostgresColumns.STUDY_ID.name}, ${PostgresColumns.LEGACY_STUDY_ID.name})
            VALUES (?, ?)
        """.trimIndent()

        /**
         * PreparedStatement bind order
         * 1) submission_id,
         * 3) study_id,
         * 4) participant_id
         * 5) submission_date,
         * 6) submission
         */
        private val INSERT_TUD_SUBMISSIONS_SQL = """
            INSERT INTO ${ChroniclePostgresTables.TIME_USE_DIARY_SUBMISSIONS.name} values (?, ?, ?, ?, ?::jsonb)
        """.trimIndent()

        private val PARTICIPANT_STATS_COLUMNS = linkedSetOf(
            PostgresColumns.STUDY_ID,
            PostgresColumns.PARTICIPANT_ID,
            PostgresColumns.ANDROID_FIRST_DATE,
            PostgresColumns.ANDROID_LAST_DATE,
            PostgresColumns.ANDROID_UNIQUE_DATES,
            PostgresColumns.TUD_FIRST_DATE,
            PostgresColumns.TUD_LAST_DATE,
            PostgresColumns.TUD_UNIQUE_DATES
        )

        /**
         * PreparedStatement binding
         * 1) studyId
         * 2) participantId,
         * 3) androidFirstDate,
         * 4) androidLastDate,
         * 5) androidUniqueDates
         * 6) tudFirstDate,
         * 7) tudLastDate
         * 8) tudUniqueDates
         */
        private val INSERT_PARTICIPANT_STATS_SQL = """
            INSERT INTO ${ChroniclePostgresTables.PARTICIPANT_STATS.name} (${PARTICIPANT_STATS_COLUMNS.joinToString { it.name }})
            VALUES (${PARTICIPANT_STATS_COLUMNS.joinToString { "?" }})
        """.trimIndent()
    }

    private val usersMap = HazelcastMap.USERS.getMap(hazelcast)

    @PostMapping(
        path = [STUDIES],
        produces = [MediaType.APPLICATION_JSON_VALUE],
        consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun importStudies(@RequestBody config: ImportStudiesConfiguration) {
        ensureAdminAccess()
        val hds = dataSourceManager.getDataSource(config.dataSourceName)
        val studiesByEkId = Maps.newConcurrentMap<UUID, Study>()
        val studiesByLegacyStudyId = Maps.newConcurrentMap<UUID, UUID>()
        val studiesByOrganizationId = Maps.newConcurrentMap<UUID, MutableSet<Study>>()
        val settingsByLegacyStudyId = Maps.newConcurrentMap<UUID, Map<String, Any>>()

        BasePostgresIterable(
            PreparedStatementHolderSupplier(hds, getStudySettingsSql(config.studySettingsTable)) {}
        ) {
            val v2StudyId: UUID? = v2StudyId(it)
            val studySettings = mapper.readValue<Map<String, Any>>(it.getString(SETTINGS.name))
            if (v2StudyId != null) {
                settingsByLegacyStudyId[v2StudyId] = studySettings
            }
        }.count()

        val studies = BasePostgresIterable(
            PreparedStatementHolderSupplier(hds, getStudiesSql(config.studiesTable)) {}
        ) {
            val v2StudyId: UUID? = v2StudyId(it)
            val study = study(it, settingsByLegacyStudyId.getOrDefault(v2StudyId, mapOf()))

            val studyId = studyService.createStudy(study)

            logger.info("Created study {}", study)
            check(study.id == studyId) { "Safety check to make sure study id got set appropriately" }

            studiesByEkId[it.getObject(V2_STUDY_EK_ID, UUID::class.java)] = study
            studiesByOrganizationId.getOrPut(it.getObject(V2_ORGANIZATION_ID, UUID::class.java)) { mutableSetOf() }
                .add(study)

            if (v2StudyId != null) {
                studiesByLegacyStudyId[v2StudyId] = study.id
            }
        }.count()

        val legacyInserts = hds.connection.use { connection ->
            connection.prepareStatement(INSERT_LEGACY_STUDY_ID_SQL).use { ps ->
                studiesByLegacyStudyId.forEach { (legacyStudyId, studyId) ->
                    var index = 1
                    ps.setObject(index++, studyId)
                    ps.setObject(index++, legacyStudyId)
                    ps.addBatch()
                }
                ps.executeBatch().sum()
            }
        }
        check(legacyInserts == studiesByLegacyStudyId.size) { "safety check to ensure all legacy study ids were inserted" }

        val participants = BasePostgresIterable(
            PreparedStatementHolderSupplier(hds, getCandidatesSql(config.candidatesTable)) {}
        ) {
            val participant = participant(it)
            val studyEkId = it.getObject(V2_STUDY_EK_ID, UUID::class.java)
            executor.submit {

                val study = studiesByEkId[studyEkId] ?: throw StudyNotFoundException(
                    studyEkId,
                    "Missing study with legacy id $studyEkId"
                )
                studyService.registerParticipant(study.id, participant)
                logger.info("Registered participant {} in study {}", participant.participantId, study)
            }
        }.map { it.get() }

        logger.info("Starting to grant permissions.")

        val legacy_users = BasePostgresIterable(
            PreparedStatementHolderSupplier(hds, getLegacyUserSql(config.usersTable!!)) {}
        ) {
            LegacyUser(
                it.getObject("participant_es_id", UUID::class.java),
                it.getString("principal_id"),
                it.getString("name"),
                it.getString("email")
            )
        }
            .filter { it.legacyEsName.startsWith("chronicle_participants_") }
            .forEach {
                it.legacyEsId = UUID.fromString(StringUtils.removeStart(it.legacyEsName, "chronicle_participants_"))
                val userStudyId = studiesByLegacyStudyId[it.legacyEsId]
                val p = tryGetPrincipal(it.principalId, it.email)
                if (userStudyId != null && p != null) {
                    val userStudy = studyService.getStudy(userStudyId)
                    authorizationManager.addPermission(
                        AclKey(userStudy.id),
                        p,
                        EnumSet.allOf(Permission::class.java)
                    )
                } else {
                    logger.warn("Unable to resolve legacy study $it")
                }
            }

        val users = BasePostgresIterable(
            PreparedStatementHolderSupplier(hds, getUserSql(config.usersTable!!)) {}
        ) {
            val organizationId = it.getObject("organization_id", UUID::class.java)
            val principalId = it.getString("principal_id")
            val principalEmail = it.getString("principal_email")

            //Except for legacy org, lookup users and grant permission on studies in that org.
            //For legacy org we will rely on granting users permissions based off of what is in permissions table
            val orgStudies = studiesByOrganizationId[organizationId] ?: setOf()
            if (organizationId != LEGACY_ORG_ID) {
                val principal = tryGetPrincipal(principalId, principalEmail)
                if (principal != null) {
                    orgStudies.forEach { orgStudy ->
                        authorizationManager.addPermission(
                            AclKey(orgStudy.id),
                            principal,
                            EnumSet.allOf(Permission::class.java)
                        )
                    }
                }
            }
        }.count()

    }

    private fun tryGetPrincipal(principalId: String, principalEmail: String): Principal? {
        if (usersMap.containsKey(principalId)) {
            val user = usersMap.getValue(principalId)
            if (user.email == principalEmail) {
                return Principal(PrincipalType.USER, principalId)
            }
        }

        val maybeUsers = usersMap.values(Predicates.equal(UserMapstore.EMAIL_INDEX, principalEmail))

        if (maybeUsers.size > 1) {
            logger.warn("Found more than 1 user with e-mail: $principalEmail, using the first")
        }

        return if (maybeUsers.isNotEmpty()) {
            Principal(PrincipalType.USER, maybeUsers.first().id)
        } else {
            logger.warn("Didn't find any users with e-mail $principalEmail... skipping")
            null
        }
    }

    @PostMapping(
        path = [PARTICIPANT_STATS],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun importParticipantStats(@RequestBody config: ImportStudiesConfiguration) {
        ensureAdminAccess()
        val hds = dataSourceManager.getDataSource(config.dataSourceName)
        val participantStats: List<ParticipantStats> = BasePostgresIterable(
            PreparedStatementHolderSupplier(hds, "SELECT * FROM ${config.participantStatsTable}") {}
        ) { ResultSetAdapters.participantStats(it) }
            .toList()
        logger.info("Retrieved ${participantStats.size} legacy participant stats entities")

        val inserts = hds.connection.use { connection ->
            connection.prepareStatement(INSERT_PARTICIPANT_STATS_SQL).use { ps ->
                participantStats.forEach {
                    val studyId = studyService.getStudyId(it.studyId)
                    if (studyId == null) {
                        logger.warn("Missing study with legacy study ${it.studyId}. skipping insert")
                        return@forEach
                    }
                    var index = 0
                    val androidUniqueDates =
                        connection.createArrayOf(PostgresDatatype.DATE.sql(), it.androidUniqueDates.toTypedArray())
                    val tudUniqueDates =
                        connection.createArrayOf(PostgresDatatype.DATE.sql(), it.tudUniqueDates.toTypedArray())

                    ps.setObject(++index, studyId)
                    ps.setString(++index, it.participantId)
                    ps.setObject(++index, it.androidFirstDate)
                    ps.setObject(++index, it.androidLastDate)
                    ps.setArray(++index, androidUniqueDates)
                    ps.setObject(++index, it.tudFirstDate)
                    ps.setObject(++index, it.tudLastDate)
                    ps.setArray(++index, tudUniqueDates)
                    ps.addBatch()
                }
                ps.executeBatch().sum()
            }
        }
        logger.info("Inserted $inserts entities into participant_stats table")
    }

    @PostMapping(
        path = [APP_USAGE_SURVEY],
        consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun importAppUsageSurvey(@RequestBody config: ImportStudiesConfiguration) {
        ensureAdminAccess()
        val hds = dataSourceManager.getDataSource(config.dataSourceName)
        val entities: List<V2AppUsageEntity> = BasePostgresIterable(
            PreparedStatementHolderSupplier(
                hds,
                "SELECT * FROM ${config.appUsageSurveyTable}"
            ) {}
        ) {
            appUsageSurvey(it)
        }.toList()

        val legacyStudyIdMapping = getLegacyStudyIdMapping(hds)

        val inserts = hds.connection.use { connection ->
            connection.prepareStatement(SurveysService.SUBMIT_APP_USAGE_SURVEY_SQL).use { ps ->
                entities.forEach {
                    val studyId = legacyStudyIdMapping[it.studyId]
                    if (studyId == null) {
                        logger.warn("Missing study with legacy studyId ${it.studyId}")
                        return@forEach
                    }
                    var index = 0
                    ps.setObject(++index, studyId)
                    ps.setString(++index, it.participant_id)
                    ps.setObject(++index, it.submissionDate)
                    ps.setString(++index, it.applicationLabel)
                    ps.setString(++index, it.appPackageName)
                    ps.setObject(++index, it.timestamp)
                    ps.setString(++index, it.timezone)
                    ps.setArray(++index, it.users)
                    ps.addBatch()
                }
                ps.executeBatch().sum()
            }
        }
        logger.info("inserted $inserts entities into app usage survey table")
    }

    @PostMapping(
        path = [SYSTEM_APPS],
        consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun importSystemApps(@RequestBody config: ImportStudiesConfiguration) {
        ensureAdminAccess()
        val hds = dataSourceManager.getDataSource(config.dataSourceName)
        hds.connection.createStatement().use { statement ->
            statement.execute("INSERT INTO ${ChroniclePostgresTables.SYSTEM_APPS.name} SELECT * FROM ${config.systemAppsTable} ON CONFLICT DO NOTHING")
        }

        // check inserts
        val inserted = BasePostgresIterable(
            PreparedStatementHolderSupplier(
                hds,
                "SELECT * FROM ${ChroniclePostgresTables.SYSTEM_APPS.name}"
            ) {}
        ) {
            ResultSetAdapters.systemApp(it)
        }.toList()

        logger.info("Inserted ${inserted.size} entities into system apps table")
    }

    @PostMapping(
        path = [TIME_USE_DIARY],
        consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun importTimeUseDiarySubmissions(@RequestBody config: ImportStudiesConfiguration) {
        ensureAdminAccess()
        val hds = dataSourceManager.getDataSource(config.dataSourceName)
        val entities = BasePostgresIterable(
            PreparedStatementHolderSupplier(
                hds,
                "SELECT * FROM ${config.timeUseDiaryTable}"
            ) {}
        ) {
            tudSubmission(it)
        }.toList()

        val inserted = hds.connection.prepareStatement(INSERT_TUD_SUBMISSIONS_SQL).use { ps ->
            entities.forEach {
                val realStudyId = studyService.getStudyId(it.study_id)
                if (realStudyId == null) {
                    logger.error("invalid study id ${it.study_id}")
                    return@forEach
                }
                var index = 0
                ps.setObject(++index, idGenerationService.getNextId())
                ps.setObject(++index, realStudyId)
                ps.setString(++index, it.participant_id)
                ps.setObject(++index, it.submission_date)
                ps.setString(++index, mapper.writeValueAsString(it.submission))
                ps.addBatch()
            }
            ps.executeBatch().sum()
        }
        logger.info("Imported $inserted time use diary submissions. Expected to import ${entities.size}")
    }

    private fun tudSubmission(rs: ResultSet): TudSubmission {
        return TudSubmission(
            study_id = rs.getObject(PostgresColumns.STUDY_ID.name, UUID::class.java),
            organization_id = rs.getObject(PostgresColumns.ORGANIZATION_ID.name, UUID::class.java),
            submission_id = rs.getObject(PostgresColumns.SUBMISSION_ID.name, UUID::class.java),
            participant_id = rs.getString(PostgresColumns.PARTICIPANT_ID.name),
            submission_date = rs.getObject(PostgresColumns.SUBMISSION_DATE.name, OffsetDateTime::class.java),
            submission = mapper.readValue(rs.getString(PostgresColumns.SUBMISSION.name))
        )
    }

    private fun participant(rs: ResultSet): Participant {
        var status = rs.getString(LEGACY_PARTICIPATION_STATUS)
        if (status == null || status == "DELETE") {
            status = ParticipationStatus.UNKNOWN.name
        }
        return Participant(
            participantId = rs.getString(LEGACY_PARTICIPANT_ID),
            candidate = Candidate(
                firstName = rs.getString(LEGACY_FIRST_NAME),
                lastName = rs.getString(LEGACY_LAST_NAME),
                dateOfBirth = rs.getObject(LEGACY_DOB, LocalDate::class.java)
            ),
            participationStatus = ParticipationStatus.valueOf(status)
        )
    }

    private fun study(rs: ResultSet, settings: Map<String, Any>?): Study {

        val v2StudyId = rs.getString(V2_STUDY_ID)
        val v2StudyEkid = rs.getString(V2_STUDY_EK_ID)

        var description = rs.getString(LEGACY_DESC)
        if (StringUtils.isBlank(description)) {
            description = ""
        }

        var title = rs.getString(LEGACY_TITLE)
        if (StringUtils.isBlank(title)) {
            title = "NO TITLE - POSSIBLY DELETED STUDY"
            description = "study_id $v2StudyId study_ekid $v2StudyEkid"
        }

        return Study(
            title = title,
            description = description,
            settings = settings ?: mapOf(),
            group = rs.getString(LEGACY_STUDY_GROUP) ?: "",
            version = rs.getString(LEGACY_STUDY_VERSION) ?: "",
            contact = rs.getString(LEGACY_STUDY_CONTACT) ?: "",
            updatedAt = rs.getObject(LEGACY_UPDATE_AT, OffsetDateTime::class.java)
        )
    }

    private fun getAppUsageTimestamp(rs: ResultSet): OffsetDateTime {
        val timezone: String = rs.getString(RedshiftColumns.TIMEZONE.name) ?: OutputConstants.DEFAULT_TIMEZONE
        val timestamp = rs.getObject(RedshiftColumns.TIMESTAMP.name, OffsetDateTime::class.java)
        val zoneId = ZoneId.of(timezone)
        return timestamp.toInstant().atZone(zoneId).toOffsetDateTime()
    }

    private fun appUsageSurvey(rs: ResultSet): V2AppUsageEntity {
        return V2AppUsageEntity(
            studyId = rs.getObject(V2_STUDY_ID, UUID::class.java),
            participant_id = rs.getString(LEGACY_PARTICIPANT_ID),
            submissionDate = rs.getObject(PostgresColumns.SUBMISSION_DATE.name, OffsetDateTime::class.java),
            applicationLabel = rs.getString(RedshiftColumns.APPLICATION_LABEL.name),
            appPackageName = rs.getString(RedshiftColumns.APP_PACKAGE_NAME.name),
            timestamp = getAppUsageTimestamp(rs),
            timezone = rs.getString(RedshiftColumns.TIMEZONE.name),
            users = rs.getArray(PostgresColumns.APP_USERS.name)
        )
    }

    private fun getLegacyStudyIdMapping(hds: HikariDataSource): Map<UUID, UUID> {
        return BasePostgresIterable(
            PreparedStatementHolderSupplier(hds, "SELECT * FROM ${LEGACY_STUDY_IDS.name}") {}
        ) { legacyStudyId(it) }
            .flatMap { it.asSequence() }
            .associate { it.key to it.value }
    }

    private fun legacyStudyId(rs: ResultSet): Map<UUID, UUID> {
        return mapOf(
            rs.getObject(
                PostgresColumns.LEGACY_STUDY_ID.name,
                UUID::class.java
            ) to rs.getObject(PostgresColumns.STUDY_ID.name, UUID::class.java)
        )
    }

    private fun v2StudyId(rs: ResultSet): UUID? {
        val v2StudyIdStr = rs.getString(V2_STUDY_ID)
        return if (StringUtils.isNotBlank(v2StudyIdStr)) UUID.fromString(v2StudyIdStr) else null
    }
}

private const val ANDROID_UNIQUE_DATES = "android_unique_dates"
private const val ANDROID_FIRST_DATE = "android_first_date"
private const val ANDROID_LAST_DATE = "android_last_date"
private const val LEGACY_DESC = "description"
private const val LEGACY_DOB = "dob"
private const val LEGACY_FIRST_NAME = "first_name"
private const val LEGACY_LAST_NAME = "last_name"
private const val LEGACY_LAT = "lat"
private const val LEGACY_LON = "lon"
private const val LEGACY_PARTICIPANT_ID = "participant_id"
private const val LEGACY_PARTICIPATION_STATUS = "participation_status"
private const val LEGACY_STUDY_CONTACT = "contact"
private const val LEGACY_STUDY_GROUP = "study_group"
private const val LEGACY_STUDY_ID = "legacy_study_id"
private const val LEGACY_STUDY_VERSION = "study_version"
private const val LEGACY_TITLE = "title"
private const val LEGACY_UPDATE_AT = "updated_at"
private const val TUD_UNIQUE_DATES = "tud_unique_dates"
private const val TUD_FIRST_DATE = "tud_first_date"
private const val TUD_LAST_DATE = "tud_last_date"
private const val V2_STUDY_EK_ID = "v2_study_ekid"
private const val V2_STUDY_ID = "v2_study_id"
private const val V2_ORGANIZATION_ID = "v2_organization_id"
private val LEGACY_ORG_ID = UUID.fromString("7349c446-2acc-4d14-b2a9-a13be39cff93")

private fun getStudiesSql(studiesTable: String): String {
    return """
        SELECT * FROM $studiesTable
    """.trimIndent()
}

private fun getCandidatesSql(candidateTable: String): String {
    return """
        SELECT * FROM $candidateTable
    """.trimIndent()
}

private fun getStudySettingsSql(studySettingsTable: String): String {
    return """
        SELECT * FROM $studySettingsTable
    """.trimIndent()
}

private fun getUserSql(usersTable: String): String {
    return """
        SELECT * FROM $usersTable
    """.trimIndent()
}

private fun getLegacyUserSql(legacyUsersTable: String): String {
    return """
        SELECT * FROM $legacyUsersTable
    """.trimIndent()
}

private data class LegacyUser(
    val participantEsId: UUID,
    val principalId: String,
    val legacyEsName: String,
    val email: String
) {
    var legacyEsId: UUID = UUID(0, 0)
}

private data class V2AppUsageEntity(
    val studyId: UUID,
    val participant_id: String,
    val submissionDate: OffsetDateTime,
    val applicationLabel: String?,
    val appPackageName: String?,
    val timestamp: OffsetDateTime,
    val timezone: String?,
    val users: Array
)

private data class TudSubmission(
    val study_id: UUID,
    val organization_id: UUID,
    val participant_id: String,
    val submission_id: UUID,
    val submission: List<TimeUseDiaryResponse>,
    val submission_date: OffsetDateTime
)
