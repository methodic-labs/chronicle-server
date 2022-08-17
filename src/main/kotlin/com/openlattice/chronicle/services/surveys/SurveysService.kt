package com.openlattice.chronicle.services.surveys

import com.codahale.metrics.annotation.Timed
import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.PostgresDatatype
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.hazelcast.core.HazelcastInstance
import com.openlattice.chronicle.android.ChronicleUsageEventType
import com.openlattice.chronicle.auditing.*
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.constants.EdmConstants
import com.openlattice.chronicle.data.LegacyChronicleQuestionnaire
import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.services.ScheduledTasksManager
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.APP_USAGE_SURVEY
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.QUESTIONNAIRES
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.QUESTIONNAIRE_SUBMISSIONS
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.FILTERED_APPS
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.SYSTEM_APPS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ACTIVE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.COMPLETED_AT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.DESCRIPTION
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.QUESTIONNAIRE_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.QUESTIONS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.QUESTION_TITLE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.RECURRENCE_RULE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.RESPONSES
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.TITLE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APPLICATION_LABEL
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_CATEGORY
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_PACKAGE_NAME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_USAGE_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.BUNDLE_IDENTIFIER
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.END_DATE_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.EVENT_TYPE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.INTERACTION_TYPE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMESTAMP
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMEZONE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.USERNAME
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.CHRONICLE_USAGE_EVENTS
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.IOS_SENSOR_DATA
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.survey.*
import com.openlattice.chronicle.util.ChronicleServerUtil.STUDY_PARTICIPANT
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.sql.Connection
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
@Service
class SurveysService(
    hazelcast: HazelcastInstance,
    private val storageResolver: StorageResolver,
    private val enrollmentManager: EnrollmentManager,
    private val scheduledTasksManager: ScheduledTasksManager,
    override val auditingManager: AuditingManager,
    val idGenerationService: HazelcastIdGenerationService,
) : SurveysManager, AuditingComponent {
    private val filteredApps = HazelcastMap.FILTERED_APPS.getMap(hazelcast)

    companion object {
        private val logger = LoggerFactory.getLogger(SurveysService::class.java)
        private val mapper = ObjectMappers.newJsonMapper()

        const val UNKNOWN_BUNDLE = "Unknown"
        private val DEVICE_USAGE_EVENT_TYPES = setOf(
            ChronicleUsageEventType.MOVE_TO_BACKGROUND.value,
            ChronicleUsageEventType.MOVE_TO_FOREGROUND.value,
            ChronicleUsageEventType.ACTIVITY_PAUSED.value,
            ChronicleUsageEventType.ACTIVITY_RESUMED.value
        )
        private val APP_USAGE_SURVEY_COLS = APP_USAGE_SURVEY.columns.joinToString(",") { it.name }
        private val APP_USAGE_SURVEY_PARAMS = APP_USAGE_SURVEY.columns.joinToString(",") { "?" }

        /**
         * PreparedStatement bind order
         * 1) studyId
         * 2) participantId
         * 3) date
         */
        val GET_APP_USAGE_SQL = """
            SELECT ${APP_PACKAGE_NAME.name}, ${APPLICATION_LABEL.name}, ${TIMESTAMP.name}, ${TIMEZONE.name}, ${EVENT_TYPE.name}
            FROM ${CHRONICLE_USAGE_EVENTS.name}
            WHERE ${STUDY_ID.name} = ?
                AND ${PARTICIPANT_ID.name} = ?
                AND ${TIMESTAMP.name} >=  ?
                AND ${TIMESTAMP.name} < ?
                AND ( ${EVENT_TYPE.name} = 1 
                      OR ${EVENT_TYPE.name} = 2
                      OR ${INTERACTION_TYPE.name} = 'Move to Foreground' 
                      OR ${INTERACTION_TYPE.name} = 'Move to Background' )
                AND (${USERNAME.name} IS NULL OR ${USERNAME.name} = '')
            ORDER BY ${TIMESTAMP.name},${APP_PACKAGE_NAME.name}
        """.trimIndent()

        const val TOTAL_USAGE_FIELD = "total_usage"
        val GET_APP_USAGE_IOS_SQL = """
            SELECT ${STUDY_ID.name}, ${PARTICIPANT_ID.name}, ${BUNDLE_IDENTIFIER.name}, sum(${APP_USAGE_TIME.name}) as $TOTAL_USAGE_FIELD
            FROM ${IOS_SENSOR_DATA.name}
            WHERE ${STUDY_ID.name} = ?
                AND ${PARTICIPANT_ID.name} = ?
                AND ${END_DATE_TIME.name} > ?
                AND ${END_DATE_TIME.name} <= ?
                AND ${APP_USAGE_TIME.name} > 0 
                AND ${BUNDLE_IDENTIFIER.name} IS NOT NULL
                AND ${BUNDLE_IDENTIFIER.name}
        """.trimIndent()

        const val CATEGORIES_FIELD = "categories"

        /**
         * 1. study id
         * 2. participant id
         * 3. start of time window
         * 4. end of time window
         */
        val GET_DEVICE_USAGE_IOS_SQL = """
            SELECT ${BUNDLE_IDENTIFIER.name}, ${APP_CATEGORY.name}, sum(${APP_USAGE_TIME.name}) as $TOTAL_USAGE_FIELD
            FROM ${IOS_SENSOR_DATA.name}
            WHERE ${STUDY_ID.name} = ?
                AND ${PARTICIPANT_ID.name} = ?
                AND ${END_DATE_TIME.name} > ?
                AND ${END_DATE_TIME.name} <= ?
                AND ${APP_USAGE_TIME.name} > 0 
                AND ${APP_CATEGORY.name} IS NOT NULL
            GROUP BY (${APP_CATEGORY.name}, ${BUNDLE_IDENTIFIER.name})
        """.trimIndent()

        /**
         * PreparedStatement bind order
         * 1) studyId
         * 2) participantId
         * 3) submission_date
         * 4) application_label
         * 5) package_name
         * 6) timestamp
         * 7) timezone
         * 8) users
         */
        val SUBMIT_APP_USAGE_SURVEY_SQL = """
            INSERT INTO ${APP_USAGE_SURVEY.name} ($APP_USAGE_SURVEY_COLS) VALUES ($APP_USAGE_SURVEY_PARAMS)
            ON CONFLICT DO NOTHING
        """.trimIndent()

        private val QUESTIONNAIRE_COLUMNS = QUESTIONNAIRES.columns.joinToString { it.name }
        private val QUESTIONNAIRE_PARAMS = QUESTIONNAIRES.columns.joinToString {
            if (it.datatype == PostgresDatatype.JSONB) "?::jsonb" else "?"
        }

        /**
         * PreparedStatement bind order
         * 1) studyId
         * 2) questionnaireId
         * 3) title
         * 4) description
         * 5) questions
         * 6) active
         * 7) date
         * 8) recurrenceRule
         */
        private val CREATE_QUESTIONNAIRE_SQL = """
            INSERT INTO ${QUESTIONNAIRES.name} (${QUESTIONNAIRE_COLUMNS}) VALUES ($QUESTIONNAIRE_PARAMS)
        """.trimIndent()

        /**
         * PreparedStatement bind order
         * 1) studyId
         * 2) questionnaireId
         */
        private val GET_QUESTIONNAIRE_SQL = """
            SELECT $QUESTIONNAIRE_COLUMNS
            FROM ${QUESTIONNAIRES.name}
            WHERE ${STUDY_ID.name} = ? AND ${QUESTIONNAIRE_ID.name} = ?
        """.trimIndent()

        /**
         * PreparedStatement bind order
         * 1) studyId
         */
        private val GET_STUDY_QUESTIONNAIRES_SQL = """
            SELECT $QUESTIONNAIRE_COLUMNS
            FROM ${QUESTIONNAIRES.name}
            WHERE ${STUDY_ID.name} = ?
        """.trimIndent()

        private val SUBMIT_QUESTIONNAIRE_COLS = QUESTIONNAIRE_SUBMISSIONS.columns.joinToString { it.name }
        private val SUBMIT_QUESTIONNAIRE_PARAMS = QUESTIONNAIRE_SUBMISSIONS.columns.joinToString {
            if (it.datatype == PostgresDatatype.JSONB) "?::jsonb" else "?"
        }

        /**
         * PreparedStatement bind order
         * 1) id
         * 2) studyId
         * 3) participantId
         * 4) questionnaireId
         * 5) completedAt
         * 6) questionTitle
         * 7) responses
         */
        private val SUBMIT_QUESTIONNAIRE_SQL = """
            INSERT INTO ${QUESTIONNAIRE_SUBMISSIONS.name} ($SUBMIT_QUESTIONNAIRE_COLS)
            VALUES ($SUBMIT_QUESTIONNAIRE_PARAMS)
        """.trimIndent()

        /**
         * 1. study id
         */
        private val DELETE_FILTERED_APPS_FOR_STUDY = """
            DELETE FROM ${FILTERED_APPS.name} WHERE ${STUDY_ID.name} = ?
        """.trimIndent()

        /**
         * 1. study id
         * 2. app package name
         */
        private val DELETE_FILTERED_APP_FOR_STUDY = """
            DELETE FROM ${FILTERED_APPS.name} WHERE ${STUDY_ID.name} = ? AND ${APP_PACKAGE_NAME.name} = ?
        """.trimIndent()

        /**
         * 1. study id
         * 2. app package name
         */
        private val INSERT_FILTERED_APP_FOR_STUDY = """
            INSERT INTO ${FILTERED_APPS.name} (${STUDY_ID.name},${APP_PACKAGE_NAME.name}) VALUES(?, ?)
        """.trimIndent()

        /**
         * 1. study id
         */
        private val INIT_FILTERED_APPS_FOR_STUDY = """
            INSERT INTO ${FILTERED_APPS.name} (${STUDY_ID.name},${APP_PACKAGE_NAME.name}) 
                SELECT ?,${APP_PACKAGE_NAME.name} FROM ${SYSTEM_APPS.name}
        """.trimIndent()

        /**
         * PreparedStatement bind order
         * 1) studyId
         * 2) questionnaireId
         */
        val GET_QUESTIONNAIRE_SUBMISSIONS_SQL = """
            SELECT ${PARTICIPANT_ID.name}, ${QUESTION_TITLE.name}, ${COMPLETED_AT.name}, ${RESPONSES.name}
            FROM ${QUESTIONNAIRE_SUBMISSIONS.name}
            WHERE ${STUDY_ID.name} = ? AND ${QUESTIONNAIRE_ID.name} = ?
        """.trimIndent()

        /**
         * PreparedStatement bind order
         * 1) studyId
         * 2) questionnaireId
         */
        val DELETE_QUESTIONNAIRE_SQL = """
            DELETE FROM ${QUESTIONNAIRES.name}
            WHERE ${STUDY_ID.name} = ? AND ${QUESTIONNAIRE_ID.name} = ?
        """.trimIndent()

        private fun getOptionalUpdateQuestionnaireSetClause(update: QuestionnaireUpdate): String {
            var result = "";
            update.description?.let { result += ", ${DESCRIPTION.name} = ? " }
            update.recurrenceRule?.let { result += ", ${RECURRENCE_RULE.name} = ?" }
            update.active?.let { result += ", ${ACTIVE.name} = ? " }
            update.questions?.let { result += ", ${QUESTIONS.name} = ?::jsonb" }

            return result
        }

        private fun getUpdateQuestionnaireSql(update: QuestionnaireUpdate): String {
            return """
            UPDATE ${QUESTIONNAIRES.name}
              SET ${TITLE.name} = ? ${getOptionalUpdateQuestionnaireSetClause(update)}
            WHERE ${STUDY_ID.name} = ? AND ${QUESTIONNAIRE_ID.name} = ?
        """.trimIndent()
        }
    }

    override fun getLegacyQuestionnaire(
        organizationId: UUID, studyId: UUID, questionnaireEKID: UUID,
    ): LegacyChronicleQuestionnaire {
        TODO("Not yet implemented")
    }

    override fun getLegacyStudyQuestionnaires(
        organizationId: UUID, studyId: UUID,
    ): Map<UUID, Map<FullQualifiedName, Set<Any>>> {
        val questionnaires = getStudyQuestionnaires(studyId)
        return questionnaires.associate { questionnaire ->
            questionnaire.id!! to mapOf(
                EdmConstants.NAME_FQN to setOf(questionnaire.title),
                EdmConstants.ACTIVE_FQN to setOf(questionnaire.active),
                EdmConstants.RRULE_FQN to setOf(questionnaire.recurrenceRule ?: ""),
                EdmConstants.DESCRIPTION_FQN to setOf(questionnaire.description)
            )
        }
    }

    override fun submitLegacyQuestionnaire(
        organizationId: UUID, studyId: UUID, participantId: String,
        questionnaireResponses: Map<UUID, Map<FullQualifiedName, Set<Any>>>,
    ) {
        TODO("Not yet implemented")
    }


    override fun submitAppUsageSurvey(
        studyId: UUID,
        participantId: String,
        surveyResponses: List<AppUsage>,
    ) {
        logger.info(
            "submitting app usage survey $STUDY_PARTICIPANT",
            studyId,
            participantId
        )

        val numWritten = writeToAppUsageTable(studyId, participantId, surveyResponses)

        if (numWritten != surveyResponses.size) {
            logger.warn("wrote {} entities but expected to write {} entities", numWritten, surveyResponses.size)
        }
    }

    /**
     * This function filters app usage data that is below the threshold for reporting.
     */
    override fun computeAggregateUsage(appUsage: List<AppUsage>): Map<String, Double> {
        val beginningOfDay =
            OffsetDateTime.now().toLocalDate().atStartOfDay(ZoneOffset.UTC.normalized()).toOffsetDateTime()

        return appUsage
            .filter { DEVICE_USAGE_EVENT_TYPES.contains(it.eventType) } // Filter out any usage events unrelated to calculated time.
            .groupBy { it.appPackageName }
            .mapValues { (_, au) ->
                //Special cases are at the beginning and end of list
                //beginning from midnight to to timestmap
                //end from last timestamp until now
                //otherwise from last move to foreground until current move to background. Beginning can be merged with reguular case
                var currentStartTime = beginningOfDay

                au.foldIndexed(0.0) { index, s, a ->
                    when (a.eventType) {
                        ChronicleUsageEventType.ACTIVITY_RESUMED.value, ChronicleUsageEventType.MOVE_TO_FOREGROUND.value -> {
                            currentStartTime = a.timestamp
                            s
                        }
                        ChronicleUsageEventType.ACTIVITY_PAUSED.value, ChronicleUsageEventType.MOVE_TO_BACKGROUND.value -> {
                            when (index) {
                                au.size - 1 -> s + ChronoUnit.SECONDS.between(a.timestamp, OffsetDateTime.now())
                                else -> s + ChronoUnit.SECONDS.between(currentStartTime, a.timestamp)
                            }
                        }
                        else -> throw IllegalStateException("Unrecognized event type.")
                    }
                }
            }
    }

    /**
     * @param studyId The study id
     * @param participantId The id of the participant.
     * @param startDateTime The start date time for the beginning of the window to retrieve
     * @param endDateTime This parameter is currently ignroed because it maybe missing in early stages of data collection.
     */
    private fun getIosDeviceUsageData(
        studyId: UUID,
        participantId: String,
        startDateTime: OffsetDateTime,
        endDateTime: OffsetDateTime,
    ): DeviceUsage {
        try {

            val (_, hds) = storageResolver.resolveAndGetFlavor(studyId)

            //category = categoryByPackage[package]
            val categoryByPackage = mutableMapOf<String, String>()

            val result = BasePostgresIterable(
                PreparedStatementHolderSupplier(hds, GET_DEVICE_USAGE_IOS_SQL) { ps ->
                    ps.setString(1, studyId.toString())
                    ps.setString(2, participantId)
                    ps.setObject(3, startDateTime)
                    ps.setObject(4, endDateTime)
                }
            ) { ResultSetAdapters.iosDeviceUsageByCategory(it) }
                .groupBy { it.category }
                .mapValues { (category, usageByCategory) ->
                    usageByCategory.fold(0.0) { totalUsage, packageUsage ->
                        categoryByPackage[packageUsage.bundleIdentifier ?: UNKNOWN_BUNDLE] = category
                        totalUsage + packageUsage.usageInSeconds
                    }
                }


            logger.info(
                "fetched {} device usage categories spanning {} to {} $STUDY_PARTICIPANT",
                result.size,
                startDateTime,
                endDateTime,
                studyId,
                participantId
            )

            return DeviceUsage(result.values.sum(), result, categoryByPackage)
        } catch (ex: Exception) {
            logger.error("unable to fetch data for app usage survey")
            throw ex
        }
    }

    override fun getDeviceUsageData(
        realStudyId: UUID,
        participantId: String,
        startDateTime: OffsetDateTime,
        endDateTime: OffsetDateTime,
    ): DeviceUsage {
        val appUsage = getAndroidAppUsageData(realStudyId, participantId, startDateTime, endDateTime)
        val filtered = computeAggregateUsage(appUsage)
        val totalTime = filtered.values.sum()
        val androidDeviceUsage = DeviceUsage(totalTime, filtered, mapOf())
        val iosDeviceUsage = getIosDeviceUsageData(realStudyId, participantId, startDateTime, endDateTime)
        return DeviceUsage(
            totalTime = androidDeviceUsage.totalTime + iosDeviceUsage.totalTime,
            iosDeviceUsage.usageByPackage + androidDeviceUsage.usageByPackage,
            androidDeviceUsage.categoryByPackage + iosDeviceUsage.categoryByPackage,
            androidDeviceUsage.users + iosDeviceUsage.users
        )
    }

    // Fetches data from UsageEvents table in redshift
    override fun getAndroidAppUsageData(
        studyId: UUID,
        participantId: String,
        startDateTime: OffsetDateTime,
        endDateTime: OffsetDateTime,
    ): List<AppUsage> {
        try {

            val (_, hds) = storageResolver.resolveAndGetFlavor(studyId)
            val filtered = filteredApps[studyId] ?: scheduledTasksManager.systemAppPackageNames

            val result = BasePostgresIterable(
                PreparedStatementHolderSupplier(hds, GET_APP_USAGE_SQL) { ps ->
                    ps.setString(1, studyId.toString())
                    ps.setString(2, participantId)
                    ps.setObject(3, startDateTime)
                    ps.setObject(4, endDateTime)
                }
            ) {
                ResultSetAdapters.appUsage(it)
            }.filterNot { filtered.contains(it.appPackageName) }


            logger.info(
                "fetched {} app usage entities spanning {} to {} $STUDY_PARTICIPANT",
                result.size,
                startDateTime,
                endDateTime,
                studyId,
                participantId
            )

            return result
        } catch (ex: Exception) {
            logger.error("unable to fetch data for app usage survey")
            throw ex
        }
    }

    override fun createQuestionnaire(studyId: UUID, questionnaire: Questionnaire): UUID {
        try {
            val questionnaireId = idGenerationService.getNextId()

            storageResolver.getPlatformStorage().connection.use { connection ->
                AuditedTransactionBuilder<Unit>(connection, auditingManager)
                    .transaction { conn ->
                        conn.prepareStatement(CREATE_QUESTIONNAIRE_SQL).use { ps ->
                            var index = 0
                            ps.setObject(++index, studyId)
                            ps.setObject(++index, questionnaireId)
                            ps.setString(++index, questionnaire.title)
                            ps.setString(++index, questionnaire.description)
                            ps.setString(++index, mapper.writeValueAsString(questionnaire.questions))
                            ps.setBoolean(++index, true)
                            ps.setObject(++index, OffsetDateTime.now())
                            ps.setString(++index, questionnaire.recurrenceRule)
                            ps.executeUpdate()
                        }
                    }
                    .audit {
                        listOf(
                            AuditableEvent(
                                AclKey(studyId),
                                eventType = AuditEventType.CREATE_QUESTIONNAIRE,
                                description = "Created questionnaire with id $questionnaireId",
                                study = studyId,
                            )
                        )
                    }
                    .buildAndRun()
            }

            return questionnaireId
        } catch (ex: Exception) {
            logger.error("unable to save questionnaire", ex)
            throw ex
        }
    }

    override fun updateQuestionnaire(studyId: UUID, questionnaireId: UUID, update: QuestionnaireUpdate) {
        try {
            storageResolver.getPlatformStorage().connection.use { connection ->
                AuditedTransactionBuilder<Int>(connection, auditingManager)
                    .transaction {
                        connection.prepareStatement(getUpdateQuestionnaireSql(update)).use { ps ->
                            var index = 0
                            ps.setString(++index, update.title)
                            update.description?.let { ps.setString(++index, it) }
                            update.recurrenceRule?.let { ps.setString(++index, it) }
                            update.active?.let { ps.setBoolean(++index, it) }
                            update.questions?.let { ps.setString(++index, mapper.writeValueAsString(it)) }
                            ps.setObject(++index, studyId)
                            ps.setObject(++index, questionnaireId)

                            ps.executeUpdate()
                        }
                    }
                    .audit {
                        listOf(
                            AuditableEvent(
                                aclKey = AclKey(studyId),
                                eventType = AuditEventType.UPDATE_QUESTIONNAIRE,
                                description = "Updated questionnaire with id $questionnaireId",
                                study = studyId
                            )
                        )
                    }
            }.buildAndRun()
        } catch (ex: Exception) {
            logger.error("unable to toggle questionnaire active status")
            throw ex
        }
    }

    override fun getQuestionnaire(studyId: UUID, questionnaireId: UUID): Questionnaire {
        try {
            val hds = storageResolver.getPlatformStorage()
            return BasePostgresIterable(
                PreparedStatementHolderSupplier(hds, GET_QUESTIONNAIRE_SQL) { ps ->
                    ps.setObject(1, studyId)
                    ps.setObject(2, questionnaireId)
                }
            ) {
                ResultSetAdapters.questionnaire(it)
            }.first()

        } catch (ex: Exception) {
            logger.error("unable to fetch questionnaire: id = $questionnaireId, studyId = $studyId")
            throw ex
        }
    }

    override fun deleteQuestionnaire(studyId: UUID, questionnaireId: UUID) {
        try {
            storageResolver.getPlatformStorage().connection.use { connection ->
                AuditedTransactionBuilder<Unit>(connection, auditingManager)
                    .transaction {
                        connection.prepareStatement(DELETE_QUESTIONNAIRE_SQL).use { ps ->
                            ps.setObject(1, studyId)
                            ps.setObject(2, questionnaireId)
                            ps.execute()
                        }
                    }.audit {
                        listOf(
                            AuditableEvent(
                                aclKey = AclKey(studyId),
                                eventType = AuditEventType.DELETE_QUESTIONNAIRE,
                                description = "Deleted questionnaire of id $questionnaireId",
                                study = studyId
                            )
                        )
                    }.buildAndRun()
            }

        } catch (ex: Exception) {
            logger.info("error deleting questionnaire $questionnaireId in study $studyId")
            throw ex
        }
    }

    override fun getStudyQuestionnaires(studyId: UUID): List<Questionnaire> {
        try {
            val hds = storageResolver.getPlatformStorage()
            return BasePostgresIterable(PreparedStatementHolderSupplier(hds, GET_STUDY_QUESTIONNAIRES_SQL) { ps ->
                ps.setObject(1, studyId)
            }) {
                ResultSetAdapters.questionnaire(it)
            }.toList()
        } catch (ex: Exception) {
            logger.error("unable fetching study $studyId questionnaires")
            throw ex
        }
    }

    override fun submitQuestionnaireResponses(
        studyId: UUID,
        participantId: String,
        questionnaireId: UUID,
        responses: List<QuestionnaireResponse>,
    ) {
        try {
            val isKnownParticipant = enrollmentManager.isKnownParticipant(studyId, participantId)
            if (!isKnownParticipant) {
                logger.error(
                    "cannot submit questionnaire because participant was not found $STUDY_PARTICIPANT",
                    studyId,
                    participantId
                )
                throw Exception("participant not found")
            }

            val hds = storageResolver.getPlatformStorage()
            val rowsWritten = writeQuestionnaireResponses(hds, studyId, participantId, questionnaireId, responses)
            logger.info(
                "recorded {} questionnaire responses", STUDY_PARTICIPANT,
                rowsWritten,
                studyId,
                participantId
            )

        } catch (ex: Exception) {
            logger.error("error submitting questionnaire responses")
            throw ex
        }
    }


    @Timed
    override fun getAppsFilteredForStudyAppUsageSurvey(studyId: UUID): Collection<String> {
        val hds = storageResolver.getPlatformStorage()
        val apps = filteredApps[studyId] ?: scheduledTasksManager.systemAppPackageNames

        hds.connection.use { connection ->
            AuditedTransactionBuilder<Unit>(connection, auditingManager)
                .transaction { }
                .audit {
                    listOf(
                        AuditableEvent(
                            aclKey = AclKey(studyId),
                            eventType = AuditEventType.GET_FILTERED_APPS,
                            description = "Retrieved list of filtered applications."
                        )
                    )
                }.buildAndRun()
        }

        return apps
    }

    @Timed
    override fun setAppsFilteredForStudyAppUsageSurvey(studyId: UUID, appPackages: Set<String>) {
        storageResolver.getPlatformStorage().connection.use { connection ->
            AuditedTransactionBuilder<Unit>(connection, auditingManager)
                .transaction { conn ->
                    val psDel = conn.prepareStatement(DELETE_FILTERED_APPS_FOR_STUDY)
                    psDel.setObject(1, studyId)
                    psDel.executeUpdate()

                    val psInsert = conn.prepareStatement(INSERT_FILTERED_APP_FOR_STUDY)
                    psInsert.setObject(1, studyId)
                    appPackages.forEach {
                        psInsert.setString(2, it)
                        psInsert.addBatch()
                    }
                    psInsert.executeBatch()
                }
                .audit {
                    listOf(
                        AuditableEvent(
                            aclKey = AclKey(studyId),
                            eventType = AuditEventType.GET_FILTERED_APPS,
                            description = "Retrieved list of filtered applications."
                        )
                    )
                }.buildAndRun()
        }
        refreshMapstore(studyId)
    }

    @Timed
    override fun filterAppForStudyAppUsageSurvey(studyId: UUID, appPackages: Set<String>) {
        storageResolver.getPlatformStorage().connection.use { connection ->
            AuditedTransactionBuilder<Unit>(connection, auditingManager)
                .transaction { conn ->
                    val psInsert = conn.prepareStatement(INSERT_FILTERED_APP_FOR_STUDY)
                    psInsert.setObject(1, studyId)
                    appPackages.forEach {
                        psInsert.setString(2, it)
                        psInsert.addBatch()
                    }
                    psInsert.executeBatch()
                }
                .audit {
                    listOf(
                        AuditableEvent(
                            aclKey = AclKey(studyId),
                            eventType = AuditEventType.FILTER_APPS,
                            description = "Retrieved list of filtered applications."
                        )
                    )
                }.buildAndRun()
        }
        refreshMapstore(studyId)
    }

    @Timed
    override fun allowAppForStudyAppUsageSurvey(studyId: UUID, appPackages: Set<String>) {
        storageResolver.getPlatformStorage().connection.use { connection ->
            AuditedTransactionBuilder<Unit>(connection, auditingManager)
                .transaction { conn ->
                    val psInsert = conn.prepareStatement(DELETE_FILTERED_APP_FOR_STUDY)
                    psInsert.setObject(1, studyId)
                    appPackages.forEach {
                        psInsert.setString(2, it)
                        psInsert.addBatch()
                    }
                    val count = psInsert.executeBatch()
                }
                .audit {
                    listOf(
                        AuditableEvent(
                            aclKey = AclKey(studyId),
                            eventType = AuditEventType.ALLOW_APPS,
                            description = "Retrieved list of filtered applications."
                        )
                    )
                }.buildAndRun()
        }
        refreshMapstore(studyId)
    }

    fun refreshMapstore(studyId: UUID) {
        filteredApps.loadAll(setOf(studyId), true)
    }

    override fun initializeFilterdApps(connection: Connection, studyId: UUID) {
        val inserted = connection.prepareStatement(INIT_FILTERED_APPS_FOR_STUDY).use { ps ->
            ps.setObject(1, studyId)
            ps.executeUpdate()
        }

        logger.info("Initialized $inserted filtered apps for study $studyId based off defaults.")
    }

    // writes questionnaire responses to postgres and returns number of rows written
    private fun writeQuestionnaireResponses(
        hds: HikariDataSource,
        studyId: UUID,
        participantId: String,
        questionnaireId: UUID,
        responses: List<QuestionnaireResponse>,
    ): Int {
        val submissionId = idGenerationService.getNextId()
        return hds.connection.use { connection ->
            try {
                val wc = connection.prepareStatement(SUBMIT_QUESTIONNAIRE_SQL).use { ps ->
                    ps.setObject(1, submissionId)
                    ps.setObject(2, studyId)
                    ps.setString(3, participantId)
                    ps.setObject(4, questionnaireId)
                    ps.setObject(5, OffsetDateTime.now())

                    responses.forEach { response ->
                        ps.setString(6, response.questionTitle)
                        ps.setArray(7, PostgresArrays.createTextArray(connection, response.value))
                        ps.addBatch()
                    }

                    ps.executeBatch().sum()
                }
                return@use wc
            } catch (ex: Exception) {
                throw ex
            }
        }
    }

    // writes survey response to postgres table
    private fun writeToAppUsageTable(studyId: UUID, participantId: String, data: List<AppUsage>): Int {

        val submissionDate = LocalDate.now()

        val hds = storageResolver.getPlatformStorage()
        return hds.connection.use { conn ->
            try {
                val wc = conn.prepareStatement(SUBMIT_APP_USAGE_SURVEY_SQL).use { ps ->
                    data.forEach { response ->
                        var index = 0
                        ps.setObject(++index, studyId)
                        ps.setString(++index, participantId)
                        ps.setObject(++index, submissionDate)
                        ps.setString(++index, response.appLabel)
                        ps.setString(++index, response.appPackageName)
                        ps.setObject(++index, response.timestamp)
                        ps.setString(++index, response.timezone)
                        ps.setArray(++index, PostgresArrays.createTextArray(conn, response.users))
                        ps.addBatch()
                    }
                    ps.executeBatch().sum()
                }
                return@use wc

            } catch (ex: Exception) {
                logger.error(
                    "unable to submit app usage survey $STUDY_PARTICIPANT",
                    studyId,
                    participantId,
                    ex
                )
                conn.rollback()
                throw ex
            }
        }
    }
}
