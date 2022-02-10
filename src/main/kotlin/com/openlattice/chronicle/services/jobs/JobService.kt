package com.openlattice.chronicle.services.jobs

import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.PostgresDatatype
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.openlattice.chronicle.storage.StorageResolver
import org.springframework.stereotype.Service
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.jobs.ChronicleJob
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.JOBS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.CONTACT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.JOB_DATA
import com.openlattice.chronicle.storage.PostgresColumns.Companion.JOB_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STATUS
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.util.*

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
@Service
class JobService (
    private val idGenerationService: HazelcastIdGenerationService,
    private val storageResolver: StorageResolver,
) : JobManager {
    companion object {
        private val logger = LoggerFactory.getLogger(JobService::class.java)
        private val mapper = ObjectMappers.newJsonMapper()

        private val JOB_COLUMNS_LIST = listOf(
            JOB_ID,
            STATUS,
            CONTACT,
            JOB_DATA,
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
    }


    override fun createJob(connection: Connection, job: ChronicleJob): UUID {
        val ps = connection.prepareStatement(INSERT_JOB_SQL)
        var index = 1
        ps.setObject(index++, job.id)
        ps.setString(index++, job.status.toString())
        ps.setString(index++, job.contact)
        ps.setString(index, mapper.writeValueAsString(job.jobData))
        ps.executeUpdate()
        return job.id
    }

    override fun getJob(jobId: UUID): ChronicleJob {
        return getJobs(listOf(jobId)).first()
    }

    override fun getJobs(jobIds: Collection<UUID>): List<ChronicleJob> {
        return BasePostgresIterable(
            PreparedStatementHolderSupplier(storageResolver.getPlatformStorage(), GET_JOBS_SQL, 256) { ps ->
                val pgJobIds = PostgresArrays.createUuidArray(ps.connection, jobIds)
                ps.setObject(1, pgJobIds)
                ps.executeQuery()
            }
        ) { ResultSetAdapters.chronicleJob(it) }.toList()
    }
}