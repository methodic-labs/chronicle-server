package com.openlattice.chronicle.jobs

import com.openlattice.chronicle.services.jobs.JobService
import org.springframework.beans.factory.annotation.Autowired

import com.google.common.eventbus.EventBus
import com.openlattice.chronicle.auditing.AuditableEvent
import com.openlattice.chronicle.auditing.AuditedOperationBuilder
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.ids.IdConstants
import com.openlattice.chronicle.storage.StorageResolver
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.util.*
import java.util.concurrent.Executors.newFixedThreadPool
import java.util.concurrent.Semaphore

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */

class BackgroundChronicleJobService(
    private val jobService: JobService,
    private val storageResolver: StorageResolver,
    private val auditingManager: AuditingManager,
) {
    private val runner = mutableMapOf<Class<*>, ChronicleJobRunner<*>>()

    companion object {
        private var MAX_AVAILABLE = 4
        private var available = Semaphore(MAX_AVAILABLE)
        private var executor = newFixedThreadPool(4)
        private val logger = LoggerFactory.getLogger(BackgroundChronicleJobService::class.java)!!
        private val NO_JOB_FOUND = (IdConstants.UNINITIALIZED.id to listOf<AuditableEvent>())
    }

    @Scheduled(fixedRate = 10_000L)
    fun tryAndAcquireTaskForExecutor() {
        logger.info("Attempting to acquire permit for executing background task.")
        val hds = storageResolver.getPlatformStorage()

        try {
            if (available.tryAcquire()) {
                logger.info("Permit acquired to execute DeleteChronicleUsageDataTask")
                executor.submit {
                    try {
                        val (jobId, _) = AuditedOperationBuilder<Pair<UUID, List<AuditableEvent>>>(
                            storageResolver.getPlatformStorage().connection,
                            auditingManager
                        )
                            .operation { connection ->
                                val job = jobService.lockAndGetNextJob(connection) ?: return@operation NO_JOB_FOUND
                                val runner = runner.getOrDefault(
                                    job.definition.javaClass,
                                    DefaultJobRunner.getDefaultJobRunner(job.definition)
                                )
                                logger.info("found a job with type = {}", job.definition.javaClass.name)
                                job.id to runner.run(connection, job)
                            }
                            .audit { it.second }
                            .buildAndRun()
                        jobService.unlockJob(jobId)
                    } finally {
                        available.release()
                    }
                }
            } else {
                logger.info("No permit acquired. Skipping DeleteChronicleUsageDataTask")
            }
        } catch (error: InterruptedException) {
            logger.info("Error acquiring permit.", error)
        }

    }

    @Autowired(required = false)
    fun registerJobHandlers(jobRunners: Set<ChronicleJobRunner<*>>) {
        jobRunners.forEach { runner -> this.runner[runner.accepts()] = runner }
    }
}