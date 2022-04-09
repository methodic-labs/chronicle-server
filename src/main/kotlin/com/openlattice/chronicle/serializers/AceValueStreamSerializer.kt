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
import com.openlattice.chronicle.authorization.AceValue
import com.openlattice.chronicle.authorization.SecurableObjectType
import com.openlattice.chronicle.hazelcast.StreamSerializerTypeIds
import com.openlattice.chronicle.serializers.DelegatedPermissionEnumSetStreamSerializer.Companion.serialize
import com.openlattice.chronicle.util.tests.TestDataFactory
import com.geekbeast.serializers.Jdk8StreamSerializers.AbstractOffsetDateTimeStreamSerializer
import com.geekbeast.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import org.springframework.stereotype.Component
import java.io.IOException
import java.time.OffsetDateTime

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class AceValueStreamSerializer : TestableSelfRegisteringStreamSerializer<AceValue> {
    override fun generateTestValue(): AceValue {
        return TestDataFactory.aceValue()
    }

    @Throws(IOException::class)
    override fun write(out: ObjectDataOutput, `object`: AceValue) {
        serialize(out, `object`.permissions)
        serialize(out, `object`.securableObjectType)
        AbstractOffsetDateTimeStreamSerializer.serialize(out, `object`.expirationDate)
    }

    @Throws(IOException::class)
    override fun read(`in`: ObjectDataInput): AceValue {
        val permissions = DelegatedPermissionEnumSetStreamSerializer.deserialize(`in`)
        val objectType = deserialize(`in`)
        val expirationDate: OffsetDateTime = AbstractOffsetDateTimeStreamSerializer.deserialize(`in`)
        return AceValue(permissions, objectType, expirationDate)
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ACE_VALUE.ordinal
    }

    override fun destroy() {}
    override fun getClazz(): Class<AceValue> {
        return AceValue::class.java
    }

    companion object {
        private val lookup = SecurableObjectType.values()
        @JvmStatic
        @Throws(IOException::class)
        fun serialize(out: ObjectDataOutput, `object`: SecurableObjectType) {
            out.writeInt(`object`.ordinal)
        }

        @JvmStatic
        @Throws(IOException::class)
        fun deserialize(`in`: ObjectDataInput): SecurableObjectType {
            return lookup[`in`.readInt()]
        }
    }
}