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
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.chronicle.authorization.Principal
import com.openlattice.chronicle.hazelcast.StreamSerializerTypeIds
import com.openlattice.chronicle.serializers.PrincipalTypeStreamSerializer.Companion.serialize
import org.springframework.stereotype.Component
import java.io.IOException

@Component
class PrincipalStreamSerializer : SelfRegisteringStreamSerializer<Principal> {
    @Throws(IOException::class)
    override fun write(out: ObjectDataOutput, `object`: Principal) {
        serialize(out, `object`)
    }

    @Throws(IOException::class)
    override fun read(`in`: ObjectDataInput): Principal {
        return deserialize(`in`)
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.PRINCIPAL.ordinal
    }

    override fun destroy() {}
    override fun getClazz(): Class<Principal> {
        return Principal::class.java
    }

    companion object {
        @JvmStatic
        @Throws(IOException::class)
        fun serialize(out: ObjectDataOutput, `object`: Principal) {
            serialize(out, `object`.type)
            out.writeUTF(`object`.id)
        }

        @JvmStatic
        @Throws(IOException::class)
        fun deserialize(`in`: ObjectDataInput): Principal {
            val type = PrincipalTypeStreamSerializer.deserialize(`in`)
            val id = `in`.readString()
            return Principal(type, id!!)
        }
    }
}