package com.openlattice.chronicle.jobs

import com.geekbeast.rhizome.jobs.JobStatus
import com.openlattice.chronicle.auditing.AuditEventType
import com.openlattice.chronicle.auditing.AuditableEvent
import com.openlattice.chronicle.auditing.AuditedOperationBuilder
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.principals.Principals
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.services.jobs.JobService
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.JOBS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.CREATED_AT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.JOB_DATA
import com.openlattice.chronicle.storage.PostgresColumns.Companion.JOB_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STATUS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.UPDATED_AT
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.CHRONICLE_USAGE_EVENTS
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
  private val jobService: JobService,
  private val storageResolver: StorageResolver,
  private val auditingManager: AuditingManager,
) {

    companion object {
        private var MAX_AVAILABLE = 4;
        private var available = Semaphore(MAX_AVAILABLE)
        private var executor = newFixedThreadPool(4)
        private val logger = LoggerFactory.getLogger(BackgroundChronicleDeletionService::class.java)!!

    }

    @Scheduled(fixedRate = 30_000L)
    fun tryAndAcquireTaskForExecutor() {
        try {
            if (!available.tryAcquire()) {
                logger.info("No permit acquired to execute DeleteChronicleUsageDataTask")
                return
            }

            logger.info("Permit acquired to execute DeleteChronicleUsageDataTask")
            executor.execute {
                try {
                    DeleteChronicleUsageDataTask(jobService, storageResolver, auditingManager).run()
                }
                catch (error: InterruptedException) {
                    logger.info("Could not acquire permit.")
                    logger.info(error.message)
                }
                finally {
                    available.release();
                    logger.info("Releasing permit for chronicle usage data deletion.")
                }
            }
        }
        catch (error: InterruptedException) {
            logger.info("Error acquiring permit: $error")
        }

    }
}