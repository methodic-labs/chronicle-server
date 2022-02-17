package com.openlattice.chronicle.services.jobs

import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.PostgresDatatype
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.geekbeast.rhizome.jobs.JobStatus
import com.openlattice.chronicle.storage.StorageResolver
import org.springframework.stereotype.Service
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.jobs.ChronicleJob
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
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.util.*
import java.util.concurrent.ConcurrentSkipListMap

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
@Service
class JobService(
    private val idGenerationService: HazelcastIdGenerationService,
    private val storageResolver: StorageResolver,
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
    }


    override fun createJob(connection: Connection, job: ChronicleJob): UUID {
        logger.info("Creating job with id = ${job.id}")
        val ps = connection.prepareStatement(INSERT_JOB_SQL)
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
        ps.executeUpdate()
        return job.id
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
}