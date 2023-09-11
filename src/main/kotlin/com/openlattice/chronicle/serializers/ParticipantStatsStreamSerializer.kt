package com.openlattice.chronicle.serializers

import com.geekbeast.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import com.geekbeast.rhizome.hazelcast.serializers.SetStreamSerializers
import com.hazelcast.internal.nio.IOUtil
import com.hazelcast.internal.serialization.impl.defaultserializers.ConstantSerializers.UuidSerializer
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.chronicle.hazelcast.StreamSerializerTypeIds
import com.openlattice.chronicle.participants.ParticipantStats
import com.openlattice.chronicle.util.tests.TestDataFactory
import org.springframework.stereotype.Component

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@getmethodic.com&gt;
 */
@Component
class ParticipantStatsStreamSerializer : TestableSelfRegisteringStreamSerializer<ParticipantStats> {
    private val uuidSerializer = UuidSerializer()
    override fun generateTestValue(): ParticipantStats = TestDataFactory.participantStats()


    override fun getTypeId(): Int = StreamSerializerTypeIds.PARTICIPANT_STATS.ordinal

    override fun read(input: ObjectDataInput): ParticipantStats {

        return ParticipantStats(
            studyId = uuidSerializer.read(input),
            participantId = input.readString()!!,
            androidLastPing = IOUtil.readOffsetDateTime(input),
            androidFirstDate = IOUtil.readOffsetDateTime(input),
            androidLastDate = IOUtil.readOffsetDateTime(input),
            androidUniqueDates = SetStreamSerializers.deserialize(input) { IOUtil.readLocalDate(it) },
            iosLastPing = IOUtil.readOffsetDateTime(input),
            iosFirstDate = IOUtil.readOffsetDateTime(input),
            iosLastDate = IOUtil.readOffsetDateTime(input),
            iosUniqueDates = SetStreamSerializers.deserialize(input) { IOUtil.readLocalDate(it) },
            tudFirstDate = IOUtil.readOffsetDateTime(input),
            tudLastDate = IOUtil.readOffsetDateTime(input),
            tudUniqueDates = SetStreamSerializers.deserialize(input) { IOUtil.readLocalDate(it) },
        )
    }

    override fun write(out: ObjectDataOutput, obj: ParticipantStats) {
        uuidSerializer.write(out, obj.studyId)
        out.writeString(obj.participantId)
        IOUtil.writeOffsetDateTime(out, obj.androidLastPing)
        IOUtil.writeOffsetDateTime(out, obj.androidFirstDate)
        IOUtil.writeOffsetDateTime(out, obj.androidLastDate)
        SetStreamSerializers.serialize(out, obj.androidUniqueDates) { output, elem ->
            IOUtil.writeLocalDate(output, elem)
        }
        IOUtil.writeOffsetDateTime(out, obj.iosLastPing)
        IOUtil.writeOffsetDateTime(out, obj.iosFirstDate)
        IOUtil.writeOffsetDateTime(out, obj.iosLastDate)
        SetStreamSerializers.serialize(out, obj.iosUniqueDates) { output, elem ->
            IOUtil.writeLocalDate(output, elem)
        }
        IOUtil.writeOffsetDateTime(out, obj.tudFirstDate)
        IOUtil.writeOffsetDateTime(out, obj.tudLastDate)
        SetStreamSerializers.serialize(out, obj.tudUniqueDates) { output, elem ->
            IOUtil.writeLocalDate(output, elem)

        }
    }

    override fun getClazz(): Class<out ParticipantStats> = ParticipantStats::class.java
}