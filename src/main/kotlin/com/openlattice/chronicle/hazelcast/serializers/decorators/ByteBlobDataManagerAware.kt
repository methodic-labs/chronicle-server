package com.openlattice.chronicle.hazelcast.serializers.decorators

import com.openlattice.chronicle.storage.ByteBlobDataManager


interface ByteBlobDataManagerAware {
    fun setByteBlobDataManager(byteBlobDataManager: ByteBlobDataManager)
}