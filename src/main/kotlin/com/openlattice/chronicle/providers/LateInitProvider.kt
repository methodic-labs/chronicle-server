package com.openlattice.chronicle.providers

import com.openlattice.chronicle.serializers.decorators.ByteBlobDataManagerAware
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.ids.IdGenerationServiceDependent
import com.openlattice.chronicle.storage.ByteBlobDataManager
import com.openlattice.chronicle.storage.StorageResolver

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface LateInitProvider : IdGenerationServiceDependent, ByteBlobDataManagerAware {
    val resolver: StorageResolver
    val idService: HazelcastIdGenerationService
    val byteBlobDataManager: ByteBlobDataManager
}