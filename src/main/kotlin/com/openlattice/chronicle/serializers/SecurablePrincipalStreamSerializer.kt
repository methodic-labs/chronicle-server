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
import com.geekbeast.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.chronicle.authorization.PrincipalType
import com.openlattice.chronicle.authorization.Role
import com.openlattice.chronicle.authorization.SecurablePrincipal
import com.openlattice.chronicle.hazelcast.StreamSerializerTypeIds
import com.openlattice.chronicle.organizations.OrganizationPrincipal
import com.openlattice.chronicle.serializers.AclKeyStreamSerializer.Companion.serialize
import com.openlattice.chronicle.serializers.PrincipalStreamSerializer.Companion.serialize
import org.springframework.stereotype.Component
import java.io.IOException
import java.util.*

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class SecurablePrincipalStreamSerializer : SelfRegisteringStreamSerializer<SecurablePrincipal> {
    override fun getClazz(): Class<out SecurablePrincipal> {
        return SecurablePrincipal::class.java
    }

    @Throws(IOException::class)
    override fun write(out: ObjectDataOutput, `object`: SecurablePrincipal) {
        serialize(out, `object`)
    }

    @Throws(IOException::class)
    override fun read(`in`: ObjectDataInput): SecurablePrincipal {
        return deserialize(`in`)
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.SECURABLE_PRINCIPAL.ordinal
    }

    override fun destroy() {}

    companion object {
        @Throws(IOException::class)
        fun serialize(out: ObjectDataOutput, `object`: SecurablePrincipal) {
            serialize(out, `object`.principal)
            serialize(out, `object`.aclKey)
            out.writeUTF(`object`.title)
            out.writeUTF(`object`.description)
        }

        @Throws(IOException::class)
        fun deserialize(`in`: ObjectDataInput): SecurablePrincipal {
            val principal = PrincipalStreamSerializer.deserialize(`in`)
            val aclKey = AclKeyStreamSerializer.deserialize(`in`)
            val title = `in`.readString()
            val description = `in`.readString()!!
            return when (principal.type) {
                PrincipalType.ROLE -> Role(
                    aclKey,
                    principal,
                    title!!,
                    Optional.of(description)
                )
                PrincipalType.ORGANIZATION -> OrganizationPrincipal(
                    aclKey,
                    principal,
                    title!!,
                    Optional.of(description)
                )
                else -> SecurablePrincipal(
                    aclKey,
                    principal,
                    title!!,
                    Optional.of(
                        description
                    )
                )
            }
        }
    }
}