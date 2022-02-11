package com.openlattice.chronicle.services.surveys

import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.openlattice.chronicle.data.ChronicleQuestionnaire
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.APP_USAGE_SURVEY
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APPLICATION_LABEL
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_PACKAGE_NAME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.RECORDED_DATE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMESTAMP
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMEZONE
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.CHRONICLE_USAGE_EVENTS
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.survey.AppUsage
import com.openlattice.chronicle.util.ChronicleServerUtil.STUDY_PARTICIPANT
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.time.LocalDate
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
                AND ${RECORDED_DATE.name} = ?
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

        if (numWritten  != surveyResponses.size) {
            logger.warn("wrote {} entities but expected to write {} entities", numWritten, surveyResponses.size)
        }
    }

    // Fetches data from UsageEvents table in redshift
    override fun getAppUsageData(studyId: UUID, participantId: String, date: String): List<AppUsage> {
        try {
            val requestedDate = LocalDate.parse(date)
            print(requestedDate.toString())

            val (_, hds) = storageResolver.resolveAndGetFlavor(studyId)

            val result = BasePostgresIterable(
                    PreparedStatementHolderSupplier(hds, GET_APP_USAGE_SQL) { ps ->
                        ps.setString(1, studyId.toString())
                        ps.setString(2, participantId)
                        ps.setObject(3, requestedDate)
                    }
            ) {
                ResultSetAdapters.appUsage(it)

            }.toList()

            logger.info(
                    "fetched {} app usage entries for date {} $STUDY_PARTICIPANT",
                    result.size,
                    requestedDate,
                    studyId,
                    participantId
            )

            return result

        } catch (ex: Exception) {
            logger.error("unable to fetch data for app usage survey")
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
