package com.openlattice.chronicle.jobs

import com.geekbeast.rhizome.jobs.JobStatus
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.storage.ChroniclePostgresTables
import com.openlattice.chronicle.storage.PostgresColumns
import com.openlattice.chronicle.storage.StorageResolver
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.sql.Connection
import java.time.OffsetDateTime
import java.util.concurrent.Executors.newFixedThreadPool
import java.util.concurrent.Semaphore

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
class BackgroundChronicleDeletionService(
  private val storageResolver: StorageResolver,
  private val auditingManager: AuditingManager,
) {

    companion object {
        private var MAX_AVAILABLE = 4;
        private var available = Semaphore(MAX_AVAILABLE)
        private var executor = newFixedThreadPool(4)
        private val logger = LoggerFactory.getLogger(BackgroundChronicleDeletionService::class.java)!!

        /*
          1. status
          2. updated_at
         */
        private val GET_NEXT_JOB_COLUMNS = listOf(
            PostgresColumns.UPDATED_AT,
            PostgresColumns.STATUS
        ).joinToString(",") { it.name }

        /**
         *  1. status
         *  2. updated_at
         */
        private val GET_NEXT_JOB_SQL = """
            UPDATE ${ChroniclePostgresTables.JOBS.name}
            SET (${GET_NEXT_JOB_COLUMNS}) = (?, ?)
            WHERE ${PostgresColumns.JOB_ID.name} = (
                SELECT ${PostgresColumns.JOB_ID.name}
                FROM ${ChroniclePostgresTables.JOBS.name}
                WHERE ${PostgresColumns.JOB_DATA.name} ->> '@type' = '${DeleteStudyUsageData::class.java.name}'
                AND ${PostgresColumns.STATUS.name} = '${JobStatus.PENDING.name}'
                ORDER BY ${PostgresColumns.CREATED_AT.name} ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING *
        """.trimIndent()

    }

    @Scheduled(fixedRate = 10_000L)
    fun tryAndAcquireTaskForExecutor() {
        try {
            if (available.tryAcquire()) {
                logger.info("Permit acquired to execute DeleteChronicleUsageDataTask")
                val job = getNextAvailableJob(storageResolver.getPlatformStorage().connection)
                var task = DeleteChronicleUsageDataTask(storageResolver, auditingManager, available)
                executor.submit(task)
            }
            else {
                logger.info("No permit acquired. Skipping DeleteChronicleUsageDataTask")
            }
        }
        catch (error: InterruptedException) {
            logger.info("Error acquiring permit: $error")
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
}