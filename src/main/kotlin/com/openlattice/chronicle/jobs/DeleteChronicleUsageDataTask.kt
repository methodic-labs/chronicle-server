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
import java.util.UUID

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
class DeleteChronicleUsageDataTask(
    private val jobService: JobService,
    private val storageResolver: StorageResolver,
    private val auditingManager: AuditingManager,
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
            STATUS,
            DELETED_ROWS
        ).joinToString(",") { it.name }

        private val UPDATE_FINISHED_DELETE_JOB_SQL = """
            UPDATE ${JOBS.name}
            SET (${UPDATE_FINISHED_JOB_COLUMNS}) = (?, ?, ?)
            WHERE ${JOB_ID.name} = ?
        """.trimIndent()
    }

    override fun run() {
        deleteChronicleUsageData()
    }

    private fun deleteChronicleUsageData() {
        val hds = storageResolver.getPlatformStorage()
        AuditedOperationBuilder<ChronicleJob>(hds.connection, auditingManager)
            .operation { connection ->
                // pull job from queue with skip locked
                // deserialize the jobData
                val job = getNextAvailableJob(connection)
                if (job.jobData is DeleteStudyUsageData) {
                    val deletedRows = deleteChronicleStudyUsageData(job.jobData)
                    // update jobData to include deletedRows
                    updateFinishedDeleteJob(connection, job.id, deletedRows)
                }
                else {
                    logger.info("No pending DeleteStudyUsageData jobs available")
                }
                return@operation job
            }
            .audit { job -> listOf(
                AuditableEvent(
                    AclKey(job.id),
                    IdConstants.SYSTEM.id,
                    DeleteChronicleUsageDataTask::class.java.name,
                    eventType = AuditEventType.BACKGROUND_USAGE_DATA_DELETION,
                    study = job.jobData.studyId
                )
            ) }
            .buildAndRun()
    }

    private fun getNextAvailableJob(connection: Connection): ChronicleJob {
        val ps = connection.prepareStatement(GET_NEXT_JOB_SQL)
        ps.setObject(1, OffsetDateTime.now())
        ps.setString(2, JobStatus.FINISHED.name)
        return ps.executeQuery().use { rs ->
            if (rs.next()) ResultSetAdapters.chronicleJob(rs)
            else ChronicleJob()
        }
    }

    private fun updateFinishedDeleteJob(connection: Connection, jobId: UUID, deletedRows: Long) {
        val ps = connection.prepareStatement(UPDATE_FINISHED_DELETE_JOB_SQL)
        var index = 1
        ps.setObject(index++, OffsetDateTime.now())
        ps.setString(index++, JobStatus.FINISHED.name)
        ps.setLong(index++, deletedRows)
        ps.setObject(index, jobId)
        ps.executeUpdate()
    }

    private fun deleteChronicleStudyUsageData(jobData: DeleteStudyUsageData): Long {
//        val (flavor, hds) = storageResolver.getDefaultEventStorage()
        logger.info("Deleting studies with id = {}", jobData.studyId)
//        val ps = hds.connection.prepareStatement(DELETE_CHRONICLE_STUDY_USAGE_DATA_SQL)
//        ps.setObject(1, jobData.studyId)
//        return ps.executeUpdate()
        return 1L
    }
}