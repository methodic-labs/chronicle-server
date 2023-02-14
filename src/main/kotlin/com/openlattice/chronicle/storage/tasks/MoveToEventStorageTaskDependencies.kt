package com.openlattice.chronicle.storage.tasks

import com.geekbeast.tasks.HazelcastTaskDependencies
import com.openlattice.chronicle.storage.StorageResolver
import com.zaxxer.hikari.HikariDataSource

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class MoveToEventStorageTaskDependencies(val storageResolver: StorageResolver) : HazelcastTaskDependencies