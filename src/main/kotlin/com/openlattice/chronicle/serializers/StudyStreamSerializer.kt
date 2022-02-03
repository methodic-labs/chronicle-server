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

import com.fasterxml.jackson.module.kotlin.readValue
import com.geekbeast.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.rhizome.hazelcast.serializers.SetStreamSerializers
import com.geekbeast.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.hazelcast.internal.serialization.impl.defaultserializers.JavaDefaultSerializers
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.chronicle.hazelcast.StreamSerializerTypeIds
import com.openlattice.chronicle.study.Study
import com.openlattice.chronicle.util.TestDataFactory
import org.springframework.stereotype.Component
import java.io.IOException

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class StudyStreamSerializer : TestableSelfRegisteringStreamSerializer<Study> {
    companion object {
        private val mapper = ObjectMappers.newJsonMapper()
        private val odtss = JavaDefaultSerializers.OffsetDateTimeSerializer()
        @JvmStatic
        @Throws(IOException::class)
        fun serialize(out: ObjectDataOutput, study:Study) {
            UUIDStreamSerializerUtils.serialize(out, study.id)
            out.writeString(study.title)
            out.writeString(study.description)

            odtss.write(out,study.createdAt)
            odtss.write(out,study.updatedAt)
            odtss.write(out,study.startedAt)
            odtss.write(out,study.endedAt)

            out.writeDouble(study.lat)
            out.writeDouble(study.lon)
            out.writeString(study.group)
            out.writeString(study.version)
            out.writeString(study.contact)
            SetStreamSerializers.fastUUIDSetSerialize(out, study.organizationIds)
            out.writeBoolean(study.notificationsEnabled)
            out.writeString(study.storage)
            out.writeByteArray(mapper.writeValueAsBytes(study.settings))

        }

        @JvmStatic
        @Throws(IOException::class)
        fun deserialize(input: ObjectDataInput): Study {
            return Study(studyId = UUIDStreamSerializerUtils.deserialize(input),
                         title = input.readString()!!,
                         description = input.readString()!!,
                         createdAt = odtss.read(input),
                         updatedAt = odtss.read(input),
                         startedAt = odtss.read(input),
                         endedAt = odtss.read(input),
                         lat = input.readDouble(),
                         lon =input.readDouble(),
                         group = input.readString()!!,
                         version = input.readString()!!,
                         contact = input.readString()!!,
                         organizationIds = SetStreamSerializers.fastUUIDSetDeserialize(input),
                         notificationsEnabled = input.readBoolean(),
                         storage = input.readString()!!,
                         settings = mapper.readValue(input.readByteArray()!!)
            )
        }
    }
    override fun generateTestValue(): Study {
        return TestDataFactory.study()
    }

    @Throws(IOException::class)
    override fun write(out: ObjectDataOutput, `object`: Study) {
        serialize(out, `object`)
    }

    @Throws(IOException::class)
    override fun read(input: ObjectDataInput): Study = deserialize(input)

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.STUDY.ordinal
    }


    override fun getClazz(): Class<Study> {
        return Study::class.java
    }
}