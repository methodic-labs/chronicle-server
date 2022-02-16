package com.openlattice.chronicle.jobs

import com.geekbeast.rhizome.jobs.JobStatus
import com.openlattice.chronicle.auditing.AuditEventType
import com.openlattice.chronicle.auditing.AuditableEvent
import com.openlattice.chronicle.auditing.AuditedOperationBuilder
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.postgres.ResultSetAdapters
import java.sql.Connection
import java.time.OffsetDateTime
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.ids.IdConstants
import com.openlattice.chronicle.services.jobs.JobService
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.JOBS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.CREATED_AT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.DELETED_ROWS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.JOB_DATA
import com.openlattice.chronicle.storage.PostgresColumns.Companion.JOB_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STATUS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.UPDATED_AT
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.CHRONICLE_USAGE_EVENTS
import com.openlattice.chronicle.storage.StorageResolver
import org.slf4j.LoggerFactory
import java.sql.PreparedStatement
import java.util.UUID
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeoutException

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
class DeleteChronicleUsageDataTask(
    private val storageResolver: StorageResolver,
    private val auditingManager: AuditingManager,
    private val available: Semaphore
): Runnable {
    companion object {
        private val logger = LoggerFactory.getLogger(DeleteChronicleUsageDataTask::class.java)!!

        /*
          1. status
          2. updated_at
         */
        private val GET_NEXT_JOB_COLUMNS = listOf(
            UPDATED_AT,
            STATUS
        ).joinToString(",") { it.name }

        private val GET_NEXT_JOB_SQL = """
            UPDATE ${JOBS.name}
            SET (${GET_NEXT_JOB_COLUMNS}) = (?, ?)
            WHERE ${JOB_ID.name} = (
                SELECT ${JOB_ID.name}
                FROM ${JOBS.name}
                WHERE ${JOB_DATA.name} ->> '@type' = '${DeleteStudyUsageData::class.java.name}'
                AND ${STATUS.name} = '${JobStatus.PENDING.name}'
                ORDER BY ${CREATED_AT.name} ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING *
        """.trimIndent()

        private val DELETE_CHRONICLE_STUDY_USAGE_DATA_SQL = """
            DELETE FROM ${CHRONICLE_USAGE_EVENTS.name}
            WHERE ${STUDY_ID.name} = ?
        """.trimIndent()

        private  val UPDATE_FINISHED_JOB_COLUMNS = listOf(
            UPDATED_AT,
            DELETED_ROWS
        ).joinToString(",") { it.name }

        private val UPDATE_FINISHED_DELETE_JOB_SQL = """
            UPDATE ${JOBS.name}
            SET (${UPDATE_FINISHED_JOB_COLUMNS}) = (?, ?)
            WHERE ${JOB_ID.name} = ?
        """.trimIndent()
    }

    override fun run() {
        try {
            logger.info("Beginning task. available = $available")
            val hds = storageResolver.getPlatformStorage()
            AuditedOperationBuilder<Unit>(hds.connection, auditingManager)
                .operation { connection ->
                    // pull job from queue with skip locked
                    // deserialize the jobData
                    val job = getNextAvailableJob(connection)
                    if (job.jobData is DeleteStudyUsageData) {
                        // delete usage data from redshift with separate connection
                        val (flavor, eventHds) = storageResolver.getDefaultEventStorage()
                        val deletedRows = deleteChronicleStudyUsageData(eventHds.connection, job.jobData)
                        eventHds.connection.close()
                        // update jobData to include deletedRows
                        updateFinishedDeleteJob(connection, job.id, deletedRows)
                    }
                    else {
                        logger.info("No pending DeleteStudyUsageData jobs available")
                    }
                }
                .audit { listOf(
                    AuditableEvent(
                        AclKey(IdConstants.SYSTEM.id),
                        IdConstants.SYSTEM.id,
                        DeleteChronicleUsageDataTask::class.java.name,
                        eventType = AuditEventType.BACKGROUND_USAGE_DATA_DELETION,
                    )
                ) }
                .buildAndRun()
        }
        catch (error: Exception) {
            logger.error("Error completing task - $error")
        }
        finally {
            logger.info("Task finalized. Releasing permit.")
            available.release()
        }
    }

    // get next available PENDING job and return as FINISHED
    // any errors will trigger rollback
    private fun getNextAvailableJob(connection: Connection): ChronicleJob {
        return connection.prepareStatement(GET_NEXT_JOB_SQL).use { ps ->
            ps.setObject(1, OffsetDateTime.now())
            ps.setString(2, JobStatus.FINISHED.name)
            return ps.executeQuery().use { rs ->
                if (rs.next()) ResultSetAdapters.chronicleJob(rs)
                else ChronicleJob()
            }
        }
    }

    // Delete chronicle study usage data from event storage and return count of deleted rows
    private fun deleteChronicleStudyUsageData(connection: Connection, jobData: DeleteStudyUsageData): Long {
        logger.info("Deleting studies with id = {}", jobData.studyId)
        return connection.prepareStatement(DELETE_CHRONICLE_STUDY_USAGE_DATA_SQL).use { ps ->
            ps.setObject(1, jobData.studyId)
            return ps.executeUpdate().toLong()
        }
    }

    // update job with number of deleted usage data rows
    private fun updateFinishedDeleteJob(connection: Connection, jobId: UUID, deletedRows: Long) {
        return connection.prepareStatement(UPDATE_FINISHED_DELETE_JOB_SQL).use { ps ->
            var index = 1
            ps.setObject(index++, OffsetDateTime.now())
            ps.setLong(index++, deletedRows)
            ps.setObject(index, jobId)
            ps.executeUpdate()
        }
    }
}