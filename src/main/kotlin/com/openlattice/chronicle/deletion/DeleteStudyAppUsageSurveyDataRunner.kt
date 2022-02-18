package com.openlattice.chronicle.deletion

import com.geekbeast.rhizome.jobs.JobStatus
import com.openlattice.chronicle.auditing.AuditEventType
import com.openlattice.chronicle.auditing.AuditableEvent
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.jobs.AbstractChronicleDeleteJobRunner
import com.openlattice.chronicle.jobs.ChronicleJob
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.APP_USAGE_SURVEY
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.time.OffsetDateTime

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
class DeleteStudyAppUsageSurveyDataRunner : AbstractChronicleDeleteJobRunner<DeleteStudyAppUsageSurveyData>() {
    companion object {
        private val logger = LoggerFactory.getLogger(DeleteStudyAppUsageSurveyDataRunner::class.java)!!

        private val DELETE_STUDY_APP_USAGE_SURVEY_DATA_SQL = """
            DELETE FROM ${APP_USAGE_SURVEY.name}
            WHERE ${STUDY_ID.name} = ?
        """.trimIndent()
    }

    override fun runJob(connection: Connection, job: ChronicleJob): List<AuditableEvent> {
        logger.info("Running delete study app usage survey data task.")
        job.definition as DeleteStudyAppUsageSurveyData

        val deletedRows = deleteAppUsageSurveyData(connection, job.definition)
        job.deletedRows = deletedRows
        job.updatedAt = OffsetDateTime.now()
        job.completedAt = job.updatedAt
        job.status = JobStatus.FINISHED

        updateFinishedDeleteJob(connection, job)

        return listOf(
            AuditableEvent(
                AclKey(job.definition.studyId),
                job.securablePrincipalId,
                job.principal,
                eventType = AuditEventType.BACKGROUND_APP_USAGE_SURVEY_DATA_DELETION,
                data = mapOf( "definition" to job.definition),
                study = job.definition.studyId
            )
        )
    }

    private fun deleteAppUsageSurveyData(connection: Connection, jobDefinition: DeleteStudyAppUsageSurveyData): Long {
        logger.info("Deleting app usage survey data with studyId = {}", jobDefinition.studyId, )
        return connection.prepareStatement(DELETE_STUDY_APP_USAGE_SURVEY_DATA_SQL).use { ps ->
            ps.setObject(1, jobDefinition.studyId)
            ps.executeUpdate().toLong()
        }
    }

    override fun accepts(): Class<DeleteStudyAppUsageSurveyData> = DeleteStudyAppUsageSurveyData::class.java
}