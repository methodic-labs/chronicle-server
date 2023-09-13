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
import java.time.OffsetDateTime

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
            androidLastPing = readDt(input),
            androidFirstDate = readDt(input),
            androidLastDate = readDt(input),
            androidUniqueDates = SetStreamSerializers.deserialize(input) { IOUtil.readLocalDate(it) },
            iosLastPing = readDt(input),
            iosFirstDate = readDt(input),
            iosLastDate = readDt(input),
            iosUniqueDates = SetStreamSerializers.deserialize(input) { IOUtil.readLocalDate(it) },
            tudFirstDate = readDt(input),
            tudLastDate = readDt(input),
            tudUniqueDates = SetStreamSerializers.deserialize(input) { IOUtil.readLocalDate(it) },
        )
    }

    override fun write(out: ObjectDataOutput, obj: ParticipantStats) {
        uuidSerializer.write(out, obj.studyId)
        out.writeString(obj.participantId)

        writeDt(out, obj.androidLastPing)
        writeDt(out, obj.androidFirstDate)
        writeDt(out, obj.androidLastDate)
        SetStreamSerializers.serialize(out, obj.androidUniqueDates) { output, elem ->
            IOUtil.writeLocalDate(output, elem)
        }
        writeDt(out, obj.iosLastPing)
        writeDt(out, obj.iosFirstDate)
        writeDt(out, obj.iosLastDate)
        SetStreamSerializers.serialize(out, obj.iosUniqueDates) { output, elem ->
            IOUtil.writeLocalDate(output, elem)
        }
        writeDt(out, obj.tudFirstDate)
        writeDt(out, obj.tudLastDate)
        SetStreamSerializers.serialize(out, obj.tudUniqueDates) { output, elem ->
            IOUtil.writeLocalDate(output, elem)

        }
    }

    private fun writeDt( out:ObjectDataOutput, obj:OffsetDateTime? ) {
        if( obj == null) {
            out.writeBoolean(false)
        } else {
            out.writeBoolean(true)
            IOUtil.writeOffsetDateTime(out, obj)
        }
    }

    private fun readDt( input: ObjectDataInput) : OffsetDateTime? {
        return if( input.readBoolean() ) {
            IOUtil.readOffsetDateTime(input)
        } else {
            null
        }
    }
    override fun getClazz(): Class<out ParticipantStats> = ParticipantStats::class.java
}