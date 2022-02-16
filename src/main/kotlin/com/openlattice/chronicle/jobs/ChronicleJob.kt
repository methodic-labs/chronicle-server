package com.openlattice.chronicle.jobs

import com.fasterxml.jackson.annotation.JsonCreator
import com.geekbeast.rhizome.jobs.AbstractDistributedJob
import com.geekbeast.rhizome.jobs.JobStatus
import com.openlattice.chronicle.ids.IdConstants
import java.time.OffsetDateTime
import java.util.UUID

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
class ChronicleJob @JsonCreator constructor(
    var id: UUID = IdConstants.UNINITIALIZED.id,
    val createdAt: OffsetDateTime = OffsetDateTime.now(),
    val updatedAt: OffsetDateTime = OffsetDateTime.now(),
    val status: JobStatus = JobStatus.PENDING,
    val contact: String = "",
    val jobData: ChronicleJobData = EmptyJobData(),
    val message: String = "",
    val deletedRows: Long = 0L
) {

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        if (!super.equals(other)) return false

        other as ChronicleJob
        if (id != other.id) return false
        if (createdAt != other.createdAt) return false
        if (updatedAt != other.updatedAt) return false
        if (status != other.status) return false
        if (contact != other.contact) return false
        if (jobData != other.jobData) return false
        if (message != other.message) return false
        if (deletedRows != other.deletedRows) return false

        return true
    }
}
