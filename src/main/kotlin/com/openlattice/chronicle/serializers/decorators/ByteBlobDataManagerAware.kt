package com.openlattice.chronicle.serializers.decorators

import com.openlattice.chronicle.storage.ByteBlobDataManager


interface ByteBlobDataManagerAware {
    fun setByteBlobDataManager(byteBlobDataManager: ByteBlobDataManager)
}