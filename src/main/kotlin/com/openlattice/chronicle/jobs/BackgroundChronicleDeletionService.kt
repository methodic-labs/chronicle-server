package com.openlattice.chronicle.jobs

import com.openlattice.chronicle.auditing.AuditedOperationBuilder
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.services.jobs.JobService
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.JOBS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.CREATED_AT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.JOB_DATA
import com.openlattice.chronicle.storage.PostgresColumns.Companion.JOB_ID
import com.openlattice.chronicle.storage.StorageResolver
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.sql.Connection

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
class BackgroundChronicleDeletionService(
  private val jobService: JobService,
  private val storageResolver: StorageResolver,
  private val auditingManager: AuditingManager,
) {
    companion object {
        private val logger = LoggerFactory.getLogger(BackgroundChronicleDeletionService::class.java)!!

        private val GET_NEXT_JOB_SQL = """
            DELETE FROM ${JOBS.name}
            WHERE ${JOB_ID.name} = (
                SELECT ${JOB_ID.name}
                FROM ${JOBS.name}
                WHERE ${JOB_DATA.name} ->> '@type' = '${DeleteStudyUsageData::class.java}'
                ORDER BY ${CREATED_AT.name} ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING *
        """.trimIndent()
    }

    @Scheduled(fixedRate = 30_000L)
    fun deleteChronicleUsageData() {
        logger.info("Starting background chronicle usage data deletion")
        val hds = storageResolver.getPlatformStorage()
        AuditedOperationBuilder<ChronicleJob>(hds.connection, auditingManager)
            .operation { connection ->
                // pull job off the queue with skip locked
                // deserialize the jobData
                val job = getNextAvailableJob(connection)
                if (job.jobData is DeleteStudyUsageData) {
                    logger.info("found a job with type = {}", job::class.java)
                }

                // make some prepared statement to delete study from
                // chronicle_usage_events and execute on event hds
                return@operation job
            }
            .audit { job -> listOf() }
            .buildAndRun()
    }

    private fun getNextAvailableJob(connection: Connection): ChronicleJob {
        return connection.use { connection ->
            val ps = connection.prepareStatement(GET_NEXT_JOB_SQL)
            ps.executeQuery().use { rs ->
                if (rs.next()) ResultSetAdapters.chronicleJob(rs)
                else ChronicleJob()
            }
        }


    }
}