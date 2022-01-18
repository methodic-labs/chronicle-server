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

import com.openlattice.chronicle.serializers.AclKeyStreamSerializer.Companion.serialize
import com.openlattice.chronicle.serializers.AclKeyStreamSerializer.Companion.deserialize
import com.openlattice.chronicle.util.InternalTestDataFactory.Companion.aclKeySet
import com.geekbeast.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import kotlin.Throws
import java.io.IOException
import com.hazelcast.nio.ObjectDataOutput
import com.geekbeast.rhizome.hazelcast.serializers.SetStreamSerializers
import com.hazelcast.nio.ObjectDataInput
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.AclKeySet
import com.openlattice.chronicle.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class AclKeySetStreamSerializer : TestableSelfRegisteringStreamSerializer<AclKeySet> {
    override fun getClazz(): Class<out AclKeySet> {
        return AclKeySet::class.java
    }

    @Throws(IOException::class)
    override fun write(out: ObjectDataOutput, `object`: AclKeySet) {
        SetStreamSerializers.serialize(out, `object`) { aclKey: AclKey? -> serialize(out, aclKey!!) }
    }

    @Throws(IOException::class)
    override fun read(`in`: ObjectDataInput): AclKeySet {
        val size = `in`.readInt()
        val aks = AclKeySet(size)
        for (i in 0 until size) {
            aks.add(deserialize(`in`))
        }
        return aks
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ACL_KEY_SET.ordinal
    }

    override fun destroy() {}
    override fun generateTestValue(): AclKeySet {
        return aclKeySet()
    }
}