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
import com.kryptnostic.rhizome.hazelcast.serializers.RhizomeUtils
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.chronicle.authorization.DelegatedPermissionEnumSet
import com.openlattice.chronicle.authorization.DelegatedPermissionEnumSet.Companion.wrap
import com.openlattice.chronicle.authorization.Permission
import com.openlattice.chronicle.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component
import java.io.IOException
import java.util.*

@Component
class DelegatedPermissionEnumSetStreamSerializer : SelfRegisteringStreamSerializer<DelegatedPermissionEnumSet?> {
    @Throws(IOException::class)
    override fun write(out: ObjectDataOutput, `object`: DelegatedPermissionEnumSet) {
        serialize(out, `object`.unwrap())
    }

    @Throws(IOException::class)
    override fun read(`in`: ObjectDataInput): DelegatedPermissionEnumSet {
        return wrap(deserialize(`in`))
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.PERMISSION_SET.ordinal
    }

    override fun destroy() {}
    override fun getClazz(): Class<DelegatedPermissionEnumSet> {
        return DelegatedPermissionEnumSet::class.java
    }

    companion object {
        @JvmStatic
        @Throws(IOException::class)
        fun serialize(out: ObjectDataOutput, `object`: EnumSet<Permission>?) {
            RhizomeUtils.Serializers.serializeEnumSet(out, Permission::class.java, `object`)
        }

        @JvmStatic
        @Throws(IOException::class)
        fun deserialize(`in`: ObjectDataInput): EnumSet<Permission> {
            return RhizomeUtils.Serializers.deSerializeEnumSet(`in`, Permission::class.java)
        }
    }
}