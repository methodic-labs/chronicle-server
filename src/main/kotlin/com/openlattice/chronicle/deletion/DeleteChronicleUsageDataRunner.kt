package com.openlattice.chronicle.deletion

import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.rhizome.jobs.JobStatus
import com.openlattice.chronicle.auditing.AuditEventType
import com.openlattice.chronicle.auditing.AuditableEvent
import com.openlattice.chronicle.authorization.AclKey
import java.sql.Connection
import java.time.OffsetDateTime
import com.openlattice.chronicle.jobs.AbstractChronicleJobRunner
import com.openlattice.chronicle.jobs.ChronicleJob
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.JOBS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.COMPLETED_AT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.DELETED_ROWS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.JOB_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.UPDATED_AT
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.CHRONICLE_USAGE_EVENTS
import com.openlattice.chronicle.storage.StorageResolver
import org.slf4j.LoggerFactory

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
class DeleteChronicleUsageDataRunner(
    private val storageResolver: StorageResolver
) : AbstractChronicleJobRunner<DeleteStudyUsageData>() {

    companion object {
        private val logger = LoggerFactory.getLogger(DeleteChronicleUsageDataRunner::class.java)!!

        private val DELETE_CHRONICLE_STUDY_USAGE_DATA_SQL = """
            DELETE FROM ${CHRONICLE_USAGE_EVENTS.name}
            WHERE ${STUDY_ID.name} = ?
        """.trimIndent()

        private val UPDATE_FINISHED_JOB_COLUMNS = listOf(
            UPDATED_AT,
            COMPLETED_AT,
            DELETED_ROWS
        ).joinToString(",") { it.name }

        private val UPDATE_FINISHED_DELETE_JOB_SQL = """
            UPDATE ${JOBS.name}
            SET ($UPDATE_FINISHED_JOB_COLUMNS) = (?, ?, ?)
            WHERE ${JOB_ID.name} = ?
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

        return listOf(
            AuditableEvent(
                AclKey((job.definition as DeleteStudyUsageData).studyId),
                job.securablePrincipalId,
                job.principal,
                eventType = AuditEventType.BACKGROUND_USAGE_DATA_DELETION,
                data = mapOf( "definition" to job.definition)
            )
        )
    }


    // Delete chronicle study usage data from event storage and return count of deleted rows
    private fun deleteChronicleStudyUsageData(connection: Connection, jobData: DeleteStudyUsageData): Long {
        logger.info("Deleting studies with id = {}", jobData.studyId)
        return connection.prepareStatement(DELETE_CHRONICLE_STUDY_USAGE_DATA_SQL).use { ps ->
            ps.setObject(1, jobData.studyId)
            ps.executeUpdate().toLong()
        }
    }

    // update job with number of deleted usage data rows
    private fun updateFinishedDeleteJob(connection: Connection, job: ChronicleJob) {
        return connection.prepareStatement(UPDATE_FINISHED_DELETE_JOB_SQL).use { ps ->
            var index = 1
            ps.setObject(index++, job.updatedAt)
            ps.setObject(index++, job.completedAt)
            ps.setLong(index++, job.deletedRows)
            ps.setObject(index, job.id)
            ps.executeUpdate()
        }
    }

    override fun accepts(): Class<DeleteStudyUsageData> = DeleteStudyUsageData::class.java

}