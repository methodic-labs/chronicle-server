package com.openlattice.chronicle.serializers

import com.geekbeast.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import com.geekbeast.rhizome.hazelcast.serializers.SetStreamSerializers
import com.hazelcast.internal.nio.IOUtil
import com.hazelcast.internal.serialization.impl.defaultserializers.ConstantSerializers.UuidSerializer
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.chronicle.hazelcast.StreamSerializerTypeIds
import com.openlattice.chronicle.mapstores.stats.ParticipantKey
import com.openlattice.chronicle.participants.ParticipantStats
import com.openlattice.chronicle.util.tests.TestDataFactory
import org.apache.commons.lang3.RandomStringUtils
import org.springframework.stereotype.Component
import java.util.UUID

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@getmethodic.com&gt;
 */
@Component
class ParticipantKeyStreamSerializer : TestableSelfRegisteringStreamSerializer<ParticipantKey> {
    private val uuidSerializer = UuidSerializer()
    override fun generateTestValue(): ParticipantKey = ParticipantKey(UUID.randomUUID(), RandomStringUtils.randomAlphanumeric(10))

    override fun getTypeId(): Int = StreamSerializerTypeIds.PARTICIPANT_KEY.ordinal

    override fun read(input: ObjectDataInput): ParticipantKey {
        return ParticipantKey(
            studyId = uuidSerializer.read(input),
            participantId = input.readString()!!
        )
    }

    override fun write(out: ObjectDataOutput, obj: ParticipantKey) {
        uuidSerializer.write(out, obj.studyId)
        out.writeString(obj.participantId)
    }

    override fun getClazz(): Class<out ParticipantKey> = ParticipantKey::class.java
}