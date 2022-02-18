package com.openlattice.chronicle.services.surveys

import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.PostgresDatatype
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.openlattice.chronicle.data.ChronicleQuestionnaire
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.APP_USAGE_SURVEY
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.QUESTIONNAIRES
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ACTIVE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.CREATED_AT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.DESCRIPTION
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.QUESTIONNAIRE_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.QUESTIONS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.TITLE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APPLICATION_LABEL
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_PACKAGE_NAME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMESTAMP
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMEZONE
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.CHRONICLE_USAGE_EVENTS
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.survey.AppUsage
import com.openlattice.chronicle.survey.Questionnaire
import com.openlattice.chronicle.util.ChronicleServerUtil.STUDY_PARTICIPANT
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.OffsetDateTime
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class SurveysService(
    private val storageResolver: StorageResolver,
) : SurveysManager {
    companion object {
        private val logger = LoggerFactory.getLogger(SurveysService::class.java)
        private val mapper = ObjectMappers.newJsonMapper()

        private val APP_USAGE_SURVEY_COLS = APP_USAGE_SURVEY.columns.joinToString(",") { it.name }
        private val APP_USAGE_SURVEY_PARAMS = APP_USAGE_SURVEY.columns.joinToString(",") { "?" }

        /**
         * PreparedStatement bind order
         * 1) studyId
         * 2) participantId
         * 3) date
         */
        // TODO: Later modify this query to only return certain event types (Move to Foreground, etc) that better represent apps that the user actually interacted with
        val GET_APP_USAGE_SQL = """
            SELECT ${APP_PACKAGE_NAME.name}, ${APPLICATION_LABEL.name}, ${TIMESTAMP.name}, ${TIMEZONE.name}
            FROM ${CHRONICLE_USAGE_EVENTS.name}
            WHERE ${STUDY_ID.name} = ?
                AND ${PARTICIPANT_ID.name} = ?
                AND ${TIMESTAMP.name} >=  ?
                AND ${TIMESTAMP.name} < ?
        """.trimIndent()

        /**
         * PreparedStatement bind order
         * 1) studyId
         * 2) participantId
         * 3) submission_date
         * 4) application_label
         * 5) package_name
         * 6) users
         * 7) timestamp
         * 8) timezone
         */
        val SUBMIT_APP_USAGE_SURVEY_SQL = """
            INSERT INTO ${APP_USAGE_SURVEY.name}($APP_USAGE_SURVEY_COLS) VALUES ($APP_USAGE_SURVEY_PARAMS)
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
         */
        private val CREATE_QUESTIONNAIRE_SQL = """
            INSERT INTO ${QUESTIONNAIRES.name}(${QUESTIONNAIRE_COLUMNS}) VALUES ($QUESTIONNAIRE_PARAMS)
        """.trimIndent()

        private val GET_QUESTIONNAIRE_COLS = setOf(QUESTIONNAIRE_ID, TITLE, DESCRIPTION, QUESTIONS, ACTIVE, CREATED_AT)
            .joinToString { it.name }

        /**
         * PreparedStatement bind order
         * 1) studyId
         * 2) questionnaireId
         */
        private val GET_QUESTIONNAIRE_SQL = """
            SELECT $GET_QUESTIONNAIRE_COLS
            FROM ${QUESTIONNAIRES.name}
            WHERE ${STUDY_ID.name} = ? AND ${QUESTIONNAIRE_ID.name} = ?
        """.trimIndent()
    }

    override fun getQuestionnaire(
        organizationId: UUID, studyId: UUID, questionnaireEKID: UUID
    ): ChronicleQuestionnaire {
        TODO("Not yet implemented")
    }

    override fun getStudyQuestionnaires(
        organizationId: UUID, studyId: UUID
    ): Map<UUID, Map<FullQualifiedName, Set<Any>>> {
        return mapOf()
    }

    override fun submitQuestionnaire(
        organizationId: UUID, studyId: UUID, participantId: String,
        questionnaireResponses: Map<UUID, Map<FullQualifiedName, Set<Any>>>
    ) {
        TODO("Not yet implemented")
    }


    override fun submitAppUsageSurvey(
        studyId: UUID,
        participantId: String,
        surveyResponses: List<AppUsage>
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

    // Fetches data from UsageEvents table in redshift
    override fun getAppUsageData(
        studyId: UUID,
        participantId: String,
        startDateTime: OffsetDateTime,
        endDateTime: OffsetDateTime
    ): List<AppUsage> {
        try {

            val (_, hds) = storageResolver.resolveAndGetFlavor(studyId)

            val result = BasePostgresIterable(
                PreparedStatementHolderSupplier(hds, GET_APP_USAGE_SQL) { ps ->
                    ps.setString(1, studyId.toString())
                    ps.setString(2, participantId)
                    ps.setObject(3, startDateTime)
                    ps.setObject(4, endDateTime)
                }
            ) {
                ResultSetAdapters.appUsage(it)

            }.toList()

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

    override fun createQuestionnaire(studyId: UUID, questionnaireId: UUID, questionnaire: Questionnaire) {
        val hds = storageResolver.getPlatformStorage()
        try {
            hds.connection.prepareStatement(CREATE_QUESTIONNAIRE_SQL).use { ps ->
                var index = 0
                ps.setObject(++index, studyId)
                ps.setObject(++index, questionnaireId)
                ps.setString(++index, questionnaire.title)
                ps.setString(++index, questionnaire.description)
                ps.setString(++index, mapper.writeValueAsString(questionnaire.questions))
                ps.setBoolean(++index, true)
                ps.setObject(++index, OffsetDateTime.now())
                ps.executeUpdate()
            }
        } catch (ex: Exception) {
            logger.error("unable to save questionnaire", ex)
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
            }.toList().first()

        } catch (ex: Exception) {
            logger.error("unable to fetch questionnaire: id = $questionnaireId, studyId = $studyId")
            throw ex
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
