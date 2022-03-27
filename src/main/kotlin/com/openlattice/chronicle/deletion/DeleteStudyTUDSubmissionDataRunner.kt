package com.openlattice.chronicle.deletion

import com.geekbeast.rhizome.jobs.JobStatus
import com.openlattice.chronicle.auditing.AuditEventType
import com.openlattice.chronicle.auditing.AuditableEvent
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.services.jobs.AbstractChronicleDeleteJobRunner
import com.openlattice.chronicle.services.jobs.ChronicleJob
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.TIME_USE_DIARY_SUBMISSIONS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.time.OffsetDateTime

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
class DeleteStudyTUDSubmissionDataRunner : AbstractChronicleDeleteJobRunner<DeleteStudyTUDSubmissionData>() {

    companion object {
        private val logger = LoggerFactory.getLogger(DeleteStudyTUDSubmissionDataRunner::class.java)!!

        private val DELETE_STUDY_TUD_DATA_SQL = """
            DELETE FROM ${TIME_USE_DIARY_SUBMISSIONS.name}
            WHERE ${STUDY_ID.name} = ?
        """.trimIndent()
    }

    override fun runJob(connection: Connection, job: ChronicleJob): List<AuditableEvent> {

        job.definition as DeleteStudyTUDSubmissionData

        val deletedRows = deleteTUDSubmissionData(connection, job.definition)

        // update jobData to include deletedRows
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
                eventType = AuditEventType.BACKGROUND_TUD_DATA_DELETION,
                data = mapOf( "definition" to job.definition),
                study = job.definition.studyId
            )
        )
    }

    private fun deleteTUDSubmissionData(connection: Connection, jobDefinition: DeleteStudyTUDSubmissionData): Long {
        logger.info("Deleting TUD submission data with studyId = {}", jobDefinition.studyId, )
        return connection.prepareStatement(DELETE_STUDY_TUD_DATA_SQL).use { ps ->
            ps.setObject(1, jobDefinition.studyId)
            ps.executeUpdate().toLong()
        }
    }

    override fun accepts(): Class<DeleteStudyTUDSubmissionData> = DeleteStudyTUDSubmissionData::class.java
}