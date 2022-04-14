package com.openlattice.chronicle.services.jobs

import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.PostgresDatatype
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.geekbeast.rhizome.jobs.JobStatus
import com.openlattice.chronicle.auditing.AuditableEvent
import com.openlattice.chronicle.auditing.AuditedOperationBuilder
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.ids.IdConstants
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.JOBS
import com.openlattice.chronicle.storage.PostgresColumns
import com.openlattice.chronicle.storage.PostgresColumns.Companion.CONTACT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.DELETED_ROWS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.JOB_DEFINITION
import com.openlattice.chronicle.storage.PostgresColumns.Companion.JOB_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.MESSAGE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PRINCIPAL_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PRINCIPAL_TYPE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SECURABLE_PRINCIPAL_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STATUS
import com.openlattice.chronicle.storage.StorageResolver
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import java.sql.Connection
import java.util.*
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
@Service
class JobService(
    private val idGenerationService: HazelcastIdGenerationService,
    private val storageResolver: StorageResolver,
    override val auditingManager: AuditingManager,
) : JobManager {
    companion object {
        private val logger = LoggerFactory.getLogger(JobService::class.java)
        private val mapper = ObjectMappers.newJsonMapper()
        private val running = ConcurrentSkipListMap<UUID, ChronicleJob>()
        private val JOB_COLUMNS_LIST = listOf(
            JOB_ID,
            SECURABLE_PRINCIPAL_ID,
            PRINCIPAL_TYPE,
            PRINCIPAL_ID,
            STATUS,
            CONTACT,
            JOB_DEFINITION,
            MESSAGE,
            DELETED_ROWS,
        )

        private val JOB_COLUMNS = JOB_COLUMNS_LIST.joinToString(",") { it.name }
        private val JOB_COLUMNS_BIND = JOB_COLUMNS_LIST.joinToString(",") {
            if (it.datatype == PostgresDatatype.JSONB) "?::jsonb" else "?"
        }
        private val INSERT_JOB_SQL = """
            INSERT INTO ${JOBS.name} ($JOB_COLUMNS) VALUES ($JOB_COLUMNS_BIND)
        """.trimIndent()
        private val GET_JOBS_SQL = """
            SELECT * FROM ${JOBS.name} WHERE ${JOB_ID.name} = ANY(?)
        """.trimIndent()

        private val GET_NEXT_JOB_SQL = """
            UPDATE ${JOBS.name}
            SET ${STATUS.name} = '${JobStatus.FINISHED.name}'
            WHERE ${JOB_ID.name} = (
                SELECT ${JOB_ID.name}
                FROM ${JOBS.name}
                WHERE ${STATUS.name} = '${JobStatus.PENDING.name}'
                ORDER BY ${PostgresColumns.CREATED_AT.name} ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING *
        """.trimIndent()

        private const val MAX_AVAILABLE = 4
        private const val FINISHED_JOB_TTL = "'7d'"

        private var available = Semaphore(MAX_AVAILABLE)
        private var executor = Executors.newFixedThreadPool(4)
        private val NO_JOB_FOUND = (IdConstants.UNINITIALIZED.id to listOf<AuditableEvent>())
        private val DELETE_FINISHED_JOBS_AFTER_TTL = """
            DELETE FROM ${JOBS.name} 
            WHERE ${STATUS.name}='${JobStatus.FINISHED.name}'
            AND ${PostgresColumns.COMPLETED_AT.name} <= now() - INTERVAL $FINISHED_JOB_TTL
        """.trimIndent()

    }

    private val runner = mutableMapOf<Class<*>, ChronicleJobRunner<*>>()

    override fun createJob(connection: Connection, job: ChronicleJob): UUID {
        return createJobs(connection, listOf(job)).first()
    }

    override fun createJobs(connection: Connection, jobs: Iterable<ChronicleJob>): Iterable<UUID> {
        val jobIds = mutableListOf<UUID>()
        connection.prepareStatement(INSERT_JOB_SQL).use { ps ->
            jobs.forEach { job ->
                jobIds.add(job.id)
                var index = 1
                ps.setObject(index++, job.id)
                ps.setObject(index++, job.securablePrincipalId)
                ps.setString(index++, job.principal.type.name)
                ps.setString(index++, job.principal.id)
                ps.setString(index++, job.status.toString())
                ps.setString(index++, job.contact)
                ps.setString(index++, mapper.writeValueAsString(job.definition))
                ps.setString(index++, job.message)
                ps.setLong(index, job.deletedRows)
                ps.addBatch()
            }
            ps.executeBatch()
        }
        tryAndAcquireTaskForExecutor()
        return jobIds
    }

    override fun lockAndGetNextJob(connection: Connection): ChronicleJob? {
        return connection.prepareStatement(GET_NEXT_JOB_SQL).use { ps ->
            ps.executeQuery().use { rs ->
                if (rs.next()) {
                    val job = ResultSetAdapters.chronicleJob(rs)
                    job.status = JobStatus.RUNNING
                    running[job.id] = job
                    job
                } else null
            }
        }
    }

    override fun unlockJob(jobId: UUID) {
        running.remove(jobId)
    }

    override fun getJob(jobId: UUID): ChronicleJob {
        return getJobs(listOf(jobId)).values.first()
    }

    override fun getJobs(jobIds: Collection<UUID>): Map<UUID, ChronicleJob> {
        return BasePostgresIterable(
            PreparedStatementHolderSupplier(storageResolver.getPlatformStorage(), GET_JOBS_SQL) { ps ->
                val pgJobIds = PostgresArrays.createUuidArray(ps.connection, jobIds)
                ps.setObject(1, pgJobIds)
                ps.executeQuery()
            }
        ) {
            val job = ResultSetAdapters.chronicleJob(it)
            job.id to (running[job.id] ?: job)
        }.toMap()
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
                                    val job = lockAndGetNextJob(connection) ?: return@operation NO_JOB_FOUND
                                    val runner = runner.getOrDefault(
                                        job.definition.javaClass,
                                        DefaultJobRunner.getDefaultJobRunner(job.definition)
                                    )
                                    logger.info("found a job with type = {}", job.definition.javaClass.name)
                                    job.id to runner.run(connection, job)
                                }
                                .audit { it.second }
                                .buildAndRun()
                            unlockJob(jobId)
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

