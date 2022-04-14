package com.openlattice.chronicle.services.jobs

import com.openlattice.chronicle.auditing.AuditingComponent
import com.openlattice.chronicle.services.jobs.ChronicleJob
import java.sql.Connection
import java.util.*

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
interface JobManager : AuditingComponent {
    fun createJob(connection: Connection, job: ChronicleJob): UUID
    fun createJobs(connection: Connection, jobs: Iterable<ChronicleJob>): Iterable<UUID>
    fun getJob(jobId: UUID): ChronicleJob
    fun getJobs(jobIds: Collection<UUID>): Map<UUID, ChronicleJob>
    fun lockAndGetNextJob(connection: Connection): ChronicleJob?
    fun unlockJob( jobId: UUID)
}