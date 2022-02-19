package com.openlattice.chronicle.deletion

import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.rhizome.jobs.JobStatus
import com.openlattice.chronicle.auditing.AuditEventType
import com.openlattice.chronicle.auditing.AuditableEvent
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.jobs.AbstractChronicleDeleteJobRunner
import com.openlattice.chronicle.jobs.ChronicleJob
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.APP_USAGE_SURVEY
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.time.OffsetDateTime

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
class DeleteParticipantAppUsageSurveyDataRunner : AbstractChronicleDeleteJobRunner<DeleteParticipantAppUsageSurveyData>() {
    companion object {
        private val logger = LoggerFactory.getLogger(DeleteParticipantAppUsageSurveyDataRunner::class.java)!!

        private val DELETE_PARTICIPANT_APP_USAGE_SURVEY_DATA_SQL = """
            DELETE FROM ${APP_USAGE_SURVEY.name}
            WHERE ${STUDY_ID.name} = ?
            AND ${PARTICIPANT_ID.name} = ANY(?)
        """.trimIndent()
    }

    override fun runJob(connection: Connection, job: ChronicleJob): List<AuditableEvent> {
        logger.info("Running delete participant app usage survey data task.")

        job.definition as DeleteParticipantTUDSubmissionData

        val deletedRows = deleteAppUsageSurveyData(connection, job.definition)

        // update jobData to include deletedRows
        job.deletedRows = deletedRows
        job.updatedAt = OffsetDateTime.now()
        job.completedAt = job.updatedAt
        job.status = JobStatus.FINISHED

        updateFinishedDeleteJob(connection, job)

        return job.definition.participantIds.map { participantId ->
            AuditableEvent(
                AclKey(participantId),
                job.securablePrincipalId,
                job.principal,
                eventType = AuditEventType.BACKGROUND_APP_USAGE_SURVEY_DATA_DELETION,
                data = mapOf( "definition" to job.definition),
                study = job.definition.studyId
            )
        }
    }

    private fun deleteAppUsageSurveyData(connection: Connection, jobDefinition: DeleteParticipantTUDSubmissionData): Long {
        logger.info("Deleting app usage survey data with studyId = {} for participantIds = {}", jobDefinition.studyId, jobDefinition.participantIds)
        return connection.prepareStatement(DELETE_PARTICIPANT_APP_USAGE_SURVEY_DATA_SQL).use { ps ->
            ps.setObject(1, jobDefinition.studyId)
            val pgParticipantIds = PostgresArrays.createUuidArray(ps.connection, jobDefinition.participantIds)
            ps.setObject(2, pgParticipantIds)
            ps.executeUpdate().toLong()
        }
    }

    override fun accepts(): Class<DeleteParticipantAppUsageSurveyData> = DeleteParticipantAppUsageSurveyData::class.java
}