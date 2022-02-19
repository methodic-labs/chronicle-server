package com.openlattice.chronicle.deletion

import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.rhizome.jobs.JobStatus
import com.openlattice.chronicle.auditing.AuditEventType
import com.openlattice.chronicle.auditing.AuditableEvent
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.jobs.AbstractChronicleDeleteJobRunner
import com.openlattice.chronicle.jobs.ChronicleJob
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.TIME_USE_DIARY_SUBMISSIONS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.time.OffsetDateTime

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
class DeleteParticipantTUDSubmissionDataRunner : AbstractChronicleDeleteJobRunner<DeleteParticipantTUDSubmissionData>() {
    companion object {
        private val logger = LoggerFactory.getLogger(DeleteParticipantTUDSubmissionDataRunner::class.java)!!

        private val DELETE_PARTICIPANT_TUD_DATA_SQL = """
            DELETE FROM ${TIME_USE_DIARY_SUBMISSIONS.name}
            WHERE ${STUDY_ID.name} = ?
            AND ${PARTICIPANT_ID.name} = ANY(?)
        """.trimIndent()
    }

    override fun runJob(connection: Connection, job: ChronicleJob): List<AuditableEvent> {
        logger.info("Running delete participant tud submissions task.")

        job.definition as DeleteParticipantTUDSubmissionData

        val deletedRows = deleteTUDSubmissionData(connection, job.definition)

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
                eventType = AuditEventType.BACKGROUND_TUD_DATA_DELETION,
                data = mapOf( "definition" to job.definition),
                study = job.definition.studyId
            )
        }
    }

    private fun deleteTUDSubmissionData(connection: Connection, jobDefinition: DeleteParticipantTUDSubmissionData): Long {
        logger.info("Deleting tud data with studyId = {} for participantIds = {}", jobDefinition.studyId, jobDefinition.participantIds)
        return connection.prepareStatement(DELETE_PARTICIPANT_TUD_DATA_SQL).use { ps ->
            ps.setObject(1, jobDefinition.studyId)
            val pgParticipantIds = PostgresArrays.createUuidArray(ps.connection, jobDefinition.participantIds)
            ps.setObject(2, pgParticipantIds)
            ps.executeUpdate().toLong()
        }
    }

    override fun accepts(): Class<DeleteParticipantTUDSubmissionData> = DeleteParticipantTUDSubmissionData::class.java
}
