package com.openlattice.chronicle.storage.tasks

import com.geekbeast.tasks.HazelcastTaskDependencies
import com.openlattice.chronicle.services.studies.StudyManager
import com.openlattice.chronicle.storage.StorageResolver

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class MoveToEventStorageTaskDependencies(
    val storageResolver: StorageResolver,
    val studyService: StudyManager,
) : HazelcastTaskDependencies