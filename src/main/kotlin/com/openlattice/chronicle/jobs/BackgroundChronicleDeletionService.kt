package com.openlattice.chronicle.jobs

import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.storage.StorageResolver
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
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

    }

    @Scheduled(fixedRate = 10_000L)
    fun tryAndAcquireTaskForExecutor() {
        try {
            if (available.tryAcquire()) {
                logger.info("Permit acquired to execute DeleteChronicleUsageDataTask")
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
}