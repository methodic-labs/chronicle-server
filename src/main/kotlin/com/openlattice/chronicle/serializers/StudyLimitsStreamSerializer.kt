/*
 * Copyright (C) 2017. OpenLattice, Inc
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */
package com.openlattice.chronicle.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.chronicle.hazelcast.StreamSerializerTypeIds
import com.geekbeast.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import com.geekbeast.rhizome.hazelcast.serializers.SetStreamSerializers
import com.openlattice.chronicle.study.StudyDuration
import com.openlattice.chronicle.study.StudyFeature
import com.openlattice.chronicle.study.StudyLimits
import org.springframework.stereotype.Component
import java.io.IOException
import java.util.*

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class StudyLimitsStreamSerializer : TestableSelfRegisteringStreamSerializer<StudyLimits> {
    companion object {
        private val lookup = StudyFeature.values()

        @JvmStatic
        @Throws(IOException::class)
        fun serializeStudyDuration(out: ObjectDataOutput, studyDuration: StudyDuration) {
            out.writeShort(studyDuration.years.toInt())
            out.writeShort(studyDuration.months.toInt())
            out.writeShort(studyDuration.days.toInt())
        }

        @JvmStatic
        @Throws(IOException::class)
        fun deserializeStudyDuration(input: ObjectDataInput): StudyDuration {
            return StudyDuration(
                input.readShort(),
                input.readShort(),
                input.readShort()
            )
        }

        @JvmStatic
        @Throws(IOException::class)
        fun serialize(out: ObjectDataOutput, `object`: StudyFeature) {
            out.writeInt(`object`.ordinal)
        }

        @JvmStatic
        @Throws(IOException::class)
        fun deserialize(`in`: ObjectDataInput): StudyFeature {
            return lookup[`in`.readInt()]
        }
    }

    override fun generateTestValue(): StudyLimits {
        return StudyLimits()
    }

    override fun getTypeId(): Int = StreamSerializerTypeIds.STUDY_LIMITS.ordinal

    override fun getClazz(): Class<out StudyLimits> {
        return StudyLimits::class.java
    }

    override fun write(out: ObjectDataOutput, obj: StudyLimits) {
        out.writeInt(obj.participantLimit)
        SetStreamSerializers.serialize(out, obj.features) { studyFeature -> serialize(out, studyFeature) }
        serializeStudyDuration(out, obj.studyDuration)
        serializeStudyDuration(out, obj.dataRetentionDuration)

    }

    override fun read(input: ObjectDataInput): StudyLimits {
        val participantLimit = input.readInt()
        val studyFeatures = EnumSet.copyOf(SetStreamSerializers.deserialize(input) { deserialize(input) })
        val studyDuration = deserializeStudyDuration(input)
        val dataRetention = deserializeStudyDuration(input)

        return StudyLimits(studyDuration, dataRetention, participantLimit, studyFeatures)
    }
}