package com.openlattice.chronicle.services.jobs

import com.geekbeast.rhizome.jobs.JobStatus
import org.springframework.beans.factory.annotation.Autowired

import com.openlattice.chronicle.auditing.AuditableEvent
import com.openlattice.chronicle.auditing.AuditedOperationBuilder
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.ids.IdConstants
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.JOBS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.COMPLETED_AT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STATUS
import com.openlattice.chronicle.storage.StorageResolver
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Async
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import java.util.*
import java.util.concurrent.Executors.newFixedThreadPool
import java.util.concurrent.Semaphore

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */

@Service
class BackgroundChronicleJobService(
    private val jobService: JobService,
    private val storageResolver: StorageResolver,
    private val auditingManager: AuditingManager,
) {
    private val runner = mutableMapOf<Class<*>, ChronicleJobRunner<*>>()

    companion object {
        private const val MAX_AVAILABLE = 4
        private const val FINISHED_JOB_TTL = "'7d'"

        private var available = Semaphore(MAX_AVAILABLE)
        private var executor = newFixedThreadPool(4)
        private val logger = LoggerFactory.getLogger(BackgroundChronicleJobService::class.java)!!
        private val NO_JOB_FOUND = (IdConstants.UNINITIALIZED.id to listOf<AuditableEvent>())
        private val DELETE_FINISHED_JOBS_AFTER_TTL = """
            DELETE FROM ${JOBS.name} 
            WHERE ${STATUS.name}='${JobStatus.FINISHED.name}'
            AND ${COMPLETED_AT.name} <= now() - INTERVAL $FINISHED_JOB_TTL
        """.trimIndent()
    }

    @Async
    fun runNow() {
        tryAndAcquireTaskForExecutor()
    }

    @Scheduled(fixedRate = 10_000L)
    fun tryAndAcquireTaskForExecutor() {
        logger.info("Attempting to acquire permit for executing background task.")

        try {
            if (available.tryAcquire()) {
                logger.info("Permit acquired to run next chronicle job")
                executor.execute {
                    try {
                        storageResolver.getPlatformStorage().connection.use { conn ->
                            val (jobId, _) = AuditedOperationBuilder<Pair<UUID, List<AuditableEvent>>>(
                                conn,
                                auditingManager
                            )
                                .operation { connection ->
                                    val job =
                                        jobService.lockAndGetNextJob(connection) ?: return@operation NO_JOB_FOUND
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
                        }
                    } catch (ex: Exception) {
                        logger.error("Task could not be completed", ex)
                    } finally {
                        available.release()
                    }
                }
            } else {
                logger.info("No permits available. Skipping chronicle job scheduling.")
            }
        } catch (ex: InterruptedException) {
            logger.error("Error acquiring permit.", ex)
        }
    }

    @Scheduled(fixedRate = 60 * 60 * 1000L)
    fun clearFinishedJobs() {
        storageResolver.getPlatformStorage().connection.use { connection ->
            val deleteCount = connection.prepareStatement(DELETE_FINISHED_JOBS_AFTER_TTL).executeUpdate()
            logger.info("Expired $deleteCount jobs.")
        }
    }

    @Autowired(required = false)
    fun registerJobHandlers(jobRunners: Set<ChronicleJobRunner<*>>) {
        jobRunners.forEach { runner -> this.runner[runner.accepts()] = runner }
    }
}