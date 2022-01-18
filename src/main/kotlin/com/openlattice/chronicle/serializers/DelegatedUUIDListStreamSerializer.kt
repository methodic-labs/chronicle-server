package com.openlattice.chronicle.serializers

import com.geekbeast.rhizome.hazelcast.serializers.ListStreamSerializers
import com.openlattice.chronicle.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class DelegatedUUIDListStreamSerializer : ListStreamSerializers.DelegatedUUIDListStreamSerializer() {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.DELEGATED_UUID_LIST.ordinal
    }
}