/*
 * Copyright (C) 2018. OpenLattice, Inc.
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
import com.geekbeast.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.chronicle.authorization.Permission
import com.openlattice.chronicle.authorization.processors.PermissionMerger
import com.openlattice.chronicle.hazelcast.StreamSerializerTypeIds
import com.openlattice.chronicle.serializers.AceValueStreamSerializer.Companion.serialize
import com.geekbeast.serializers.Jdk8StreamSerializers.AbstractOffsetDateTimeStreamSerializer
import org.springframework.stereotype.Component
import java.io.IOException
import java.time.OffsetDateTime
import java.util.*

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class PermissionMergerStreamSerializer : SelfRegisteringStreamSerializer<PermissionMerger?> {
    override fun getClazz(): Class<out PermissionMerger> {
        return PermissionMerger::class.java
    }

    @Throws(IOException::class)
    override fun write(
        out: ObjectDataOutput, `object`: PermissionMerger
    ) {
        serialize(out, `object`.backingCollection)
        serialize(out, `object`.securableObjectType)
        AbstractOffsetDateTimeStreamSerializer.serialize(out, `object`.expirationDate)
    }

    @Throws(IOException::class)
    override fun read(`in`: ObjectDataInput): PermissionMerger {
        val ps = deserialize(`in`)
        val securableObjectType = AceValueStreamSerializer.deserialize(`in`)
        val expirationDate: OffsetDateTime = AbstractOffsetDateTimeStreamSerializer.deserialize(`in`)
        return PermissionMerger(ps, securableObjectType, expirationDate)
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.PERMISSION_MERGER.ordinal
    }

    override fun destroy() {}

    companion object {
        private val P = Permission.values()
        @Throws(IOException::class)
        fun deserialize(`in`: ObjectDataInput): EnumSet<Permission> {
            val bs = BitSet.valueOf(`in`.readLongArray()!!)
            val ps = EnumSet.noneOf(Permission::class.java)
            for (i in P.indices) {
                if (bs[i]) {
                    ps.add(P[i])
                }
            }
            return ps
        }

        @Throws(IOException::class)
        fun serialize(out: ObjectDataOutput, `object`: Iterable<Permission>) {
            val bs = BitSet(P.size)
            for (p in `object`) {
                bs.set(p.ordinal)
            }
            out.writeLongArray(bs.toLongArray())
            //TODO: Move this method to class where it's not as hidden
        }
    }
}