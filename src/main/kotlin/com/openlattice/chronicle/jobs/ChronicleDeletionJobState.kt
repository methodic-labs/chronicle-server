package com.openlattice.chronicle.jobs

import com.geekbeast.rhizome.jobs.JobState
import java.util.*

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
data class ChronicleDeletionJobState(
    val targetType: String,
    val targetId: UUID,
    val storage: String,
    internal var totalToDelete: Long = 0,
    val numDeletes: Long = 0,
) : JobState
