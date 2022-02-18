package com.openlattice.chronicle.deletion

import com.geekbeast.rhizome.jobs.JobStatus
import com.openlattice.chronicle.auditing.AuditEventType
import com.openlattice.chronicle.auditing.AuditableEvent
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.jobs.AbstractChronicleDeleteJobRunner
import com.openlattice.chronicle.jobs.ChronicleJob
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.CHRONICLE_USAGE_EVENTS
import com.openlattice.chronicle.storage.StorageResolver
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.time.OffsetDateTime

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
class DeleteStudyUsageDataRunner(
    private val storageResolver: StorageResolver
) : AbstractChronicleDeleteJobRunner<DeleteStudyUsageData>() {

    companion object {
        private val logger = LoggerFactory.getLogger(DeleteStudyUsageDataRunner::class.java)!!

        private val DELETE_CHRONICLE_STUDY_USAGE_DATA_SQL = """
            DELETE FROM ${CHRONICLE_USAGE_EVENTS.name}
            WHERE ${STUDY_ID.name} = ?
        """.trimIndent()
    }

    override fun runJob(connection: Connection, job: ChronicleJob): List<AuditableEvent> {
        logger.info("Running delete study usage events task.")
        // delete usage data from redshift with separate connection
        val (flavor, eventHds) = storageResolver.getDefaultEventStorage()
        val deletedRows = eventHds.connection.use { eventConnection ->
            deleteChronicleStudyUsageData(eventConnection, job.definition as DeleteStudyUsageData)
        }

        // update jobData to include deletedRows
        job.deletedRows = deletedRows
        job.updatedAt = OffsetDateTime.now()
        job.completedAt = job.updatedAt
        job.status = JobStatus.FINISHED

        updateFinishedDeleteJob(connection, job)

        val studyId = (job.definition as DeleteStudyUsageData).studyId

        return listOf(
            AuditableEvent(
                AclKey(studyId),
                job.securablePrincipalId,
                job.principal,
                eventType = AuditEventType.BACKGROUND_USAGE_DATA_DELETION,
                data = mapOf( "definition" to job.definition),
                study = studyId
            )
        )
    }


    // Delete chronicle study usage data from event storage and return count of deleted rows
    private fun deleteChronicleStudyUsageData(connection: Connection, jobData: DeleteStudyUsageData): Long {
        logger.info("Deleting usage data with studyId = {}", jobData.studyId)
        return connection.prepareStatement(DELETE_CHRONICLE_STUDY_USAGE_DATA_SQL).use { ps ->
            ps.setObject(1, jobData.studyId)
            ps.executeUpdate().toLong()
        }
    }

    override fun accepts(): Class<DeleteStudyUsageData> = DeleteStudyUsageData::class.java

}
