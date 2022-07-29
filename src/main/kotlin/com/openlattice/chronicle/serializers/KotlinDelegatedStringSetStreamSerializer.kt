package com.openlattice.chronicle.serializers

import com.geekbeast.hazelcast.serializers.AbstractKotlinDelegatedStringSet
import com.openlattice.chronicle.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@getmethodic.com&gt;
 */
@Component
class KotlinDelegatedStringSetStreamSerializer : AbstractKotlinDelegatedStringSet() {
    override fun getTypeId(): Int = StreamSerializerTypeIds.KOTLIN_DELEGATED_STRING_SET.ordinal
}