package com.openlattice.chronicle.services.surveys

import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.openlattice.chronicle.data.ChronicleQuestionnaire
import com.openlattice.chronicle.postgres.ResultSetAdapters
//import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.APPS_USAGE
//import com.openlattice.chronicle.storage.PostgresColumns.Companion.APP_LABEL
//import com.openlattice.chronicle.storage.PostgresColumns.Companion.APP_PACKAGE_NAME
//import com.openlattice.chronicle.storage.PostgresColumns.Companion.APP_USAGE_DATE
//import com.openlattice.chronicle.storage.PostgresColumns.Companion.APP_USAGE_ID
//import com.openlattice.chronicle.storage.PostgresColumns.Companion.APP_USAGE_TIMESTAMP
//import com.openlattice.chronicle.storage.PostgresColumns.Companion.APP_USAGE_TIMEZONE
//import com.openlattice.chronicle.storage.PostgresColumns.Companion.APP_USAGE_USERS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.survey.AppUsage
import com.openlattice.chronicle.util.ChronicleServerUtil.ORG_STUDY_PARTICIPANT
import com.openlattice.chronicle.util.ensureVanilla
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

        /**
         * PreparedStatement bind order
         * 1) organizationId
         * 2) studyId
         * 3) participantId
         * 4) date
         */
//        val GET_APP_USAGE_SQL = """
//            SELECT ${APP_USAGE_ID.name}, ${APP_USAGE_TIMEZONE.name}, ${APP_PACKAGE_NAME.name}, ${APP_LABEL.name}, ${APP_USAGE_TIMESTAMP.name}
//            FROM ${APPS_USAGE.name}
//            WHERE ${ORGANIZATION_ID.name} = ?
//                AND ${STUDY_ID.name} = ?
//                AND ${PARTICIPANT_ID.name} = ?
//                AND ${APP_USAGE_DATE.name} = ?
//                AND (cardinality(${APP_USAGE_USERS.name}) = 0 OR ${APP_USAGE_USERS.name} IS NULL)
//        """.trimIndent()

        /**
         * PreparedStatement bind order
         * 1) appUsers
         * 2) organizationId
         * 3) studyId
         * 4) participantId
         * 5) appUsageId
         */
//        val SUBMIT_APP_USAGE_SURVEY_SQL = """
//            UPDATE ${APPS_USAGE.name}
//            SET ${APP_USAGE_USERS.name} = ?
//            WHERE ${ORGANIZATION_ID.name} = ?
//                AND ${STUDY_ID.name} = ?
//                AND ${PARTICIPANT_ID.name} = ?
//                AND ${APP_USAGE_ID.name} = ?
//        """.trimIndent()
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
            organizationId: UUID,
            studyId: UUID,
            participantId: String,
            surveyResponses: Map<UUID, Set<String>>
    ) {
        logger.info(
                "submitting app usage survey $ORG_STUDY_PARTICIPANT",
                organizationId,
                studyId,
                participantId
        )

        val numWritten = updateAppUsage(organizationId, studyId, participantId, surveyResponses)

        if (numWritten  != surveyResponses.size) {
            logger.warn("updated {} entities but expected to update {} entities", numWritten, surveyResponses.size)
        }
    }

    override fun getAppUsageData(organizationId: UUID, studyId: UUID, participantId: String, date: String): List<AppUsage> {
//        try {
//            val requestedDate = LocalDate.parse(date)
//
//            val (flavor, hds) = storageResolver.resolveAndGetFlavor(studyId)
//            ensureVanilla(flavor)
//
//            val result = BasePostgresIterable(
//                    PreparedStatementHolderSupplier(hds, GET_APP_USAGE_SQL) { ps ->
//                        ps.setObject(1, organizationId)
//                        ps.setObject(2, studyId)
//                        ps.setObject(3, participantId)
//                        ps.setObject(4, requestedDate)
//                    }
//            ) {
//                ResultSetAdapters.appUsage(it)
//
//            }.toList()
//
//            logger.info(
//                    "fetched {} app usage entries for date {} $ORG_STUDY_PARTICIPANT",
//                    result.size,
//                    requestedDate,
//                    organizationId,
//                    studyId,
//                    participantId
//            )
//
//            return result
//
//        } catch (ex: Exception) {
//            logger.error("unable to parse date string")
//            throw ex
//        }
        return listOf()
    }

    private fun updateAppUsage( organizationId: UUID, studyId: UUID, participantId: String, data: Map<UUID, Set<String>>): Int {
        val (flavor, hds) = storageResolver.resolveAndGetFlavor(studyId)
        ensureVanilla(flavor)

//        return hds.connection.use { conn ->
//            try {
//                val wc = conn.prepareStatement(SUBMIT_APP_USAGE_SURVEY_SQL).use { ps ->
//                    data.forEach { (appUsageId, appUsers) ->
//                        ps.setArray(1, PostgresArrays.createTextArray(conn, appUsers))
//                        ps.setObject(2, organizationId)
//                        ps.setObject(3, studyId)
//                        ps.setString(4, participantId)
//                        ps.setObject(5, appUsageId)
//                        ps.addBatch()
//                    }
//                    ps.executeBatch().sum()
//                }
//                return@use wc
//
//            } catch (ex: Exception) {
//                logger.error(
//                        "unable to submit app usage survey $ORG_STUDY_PARTICIPANT",
//                        organizationId,
//                        studyId,
//                        participantId,
//                        ex
//                )
//                conn.rollback()
//                throw ex
//            }
//        }
        return 0
    }
}
