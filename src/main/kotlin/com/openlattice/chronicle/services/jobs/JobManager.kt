package com.openlattice.chronicle.services.jobs

import com.openlattice.chronicle.jobs.ChronicleJob
import java.sql.Connection
import java.util.*

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
interface JobManager {
    fun createJob(connection: Connection, job: ChronicleJob): UUID
    fun getJob(jobId: UUID): ChronicleJob
    fun getJobs(jobIds: Collection<UUID>): List<ChronicleJob>
}