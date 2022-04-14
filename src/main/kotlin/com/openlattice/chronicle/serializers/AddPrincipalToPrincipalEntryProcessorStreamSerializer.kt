package com.openlattice.chronicle.serializers

import com.geekbeast.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.principals.processors.AddPrincipalToPrincipalEntryProcessor
import com.openlattice.chronicle.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@getmethodic.com&gt;
 */
@Component
class AddPrincipalToPrincipalEntryProcessorStreamSerializer: TestableSelfRegisteringStreamSerializer<AddPrincipalToPrincipalEntryProcessor> {
    override fun generateTestValue(): AddPrincipalToPrincipalEntryProcessor  = AddPrincipalToPrincipalEntryProcessor(
        AclKey(UUID.randomUUID()) )

    override fun getTypeId(): Int = StreamSerializerTypeIds.ADD_PRINCIPAL_TO_PRINCIPAL_EP.ordinal

    override fun getClazz(): Class<out AddPrincipalToPrincipalEntryProcessor>  = AddPrincipalToPrincipalEntryProcessor::class.java

    override fun write(out: ObjectDataOutput, `object`: AddPrincipalToPrincipalEntryProcessor) {
        AclKeyStreamSerializer.serialize(out, `object`.getAclKey())
    }

    override fun read(`in`: ObjectDataInput): AddPrincipalToPrincipalEntryProcessor {
         return AddPrincipalToPrincipalEntryProcessor(AclKeyStreamSerializer.deserialize(`in`) )
    }
}