package com.openlattice.chronicle.providers

import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.providers.LateInitProvider
import com.openlattice.chronicle.storage.ByteBlobDataManager
import com.openlattice.chronicle.storage.StorageResolver

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class OnUseLateInitProvider : LateInitProvider {
    private lateinit var _resolver: StorageResolver
    private lateinit var _idService: HazelcastIdGenerationService
    private lateinit var _byteBlobDataManager: ByteBlobDataManager

    override val resolver: StorageResolver
        get() = _resolver
    override val idService: HazelcastIdGenerationService
        get() = _idService
    override val byteBlobDataManager: ByteBlobDataManager
        get() = _byteBlobDataManager

    override fun init(idService: HazelcastIdGenerationService) {
        _idService = idService
    }

    override fun setByteBlobDataManager(byteBlobDataManager: ByteBlobDataManager) {
        _byteBlobDataManager = byteBlobDataManager
    }

    fun setStorageResolver(resolver: StorageResolver) {
        _resolver = resolver
    }

}