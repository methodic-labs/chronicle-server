package com.openlattice.chronicle.jobs

import com.fasterxml.jackson.annotation.JsonCreator
import com.geekbeast.rhizome.jobs.AbstractDistributedJob
import com.geekbeast.rhizome.jobs.JobStatus
import java.util.*

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
class ChronicleDeletionJob(
    state: ChronicleDeletionJobState
) : AbstractDistributedJob<Long, ChronicleDeletionJobState>(state) {

    @JsonCreator
    constructor(
        id: UUID?,
        taskId: Long?,
        status: JobStatus,
        progress: Byte,
        hasWorkRemaining: Boolean,
        result: Long?,
        state: ChronicleDeletionJobState
    ) : this(state) {
        initialize(id, taskId, status, progress, hasWorkRemaining, result)
    }

    override fun processNextBatch() {
        TODO("Not yet implemented")
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        if (!super.equals(other)) return false

        other as ChronicleDeletionJob

        return true
    }
}
