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
import com.openlattice.chronicle.authorization.AceKey
import com.openlattice.chronicle.hazelcast.StreamSerializerTypeIds
import com.openlattice.chronicle.util.TestDataFactory
import com.geekbeast.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import org.springframework.stereotype.Component
import java.io.IOException

@Component
class AceKeyStreamSerializer : TestableSelfRegisteringStreamSerializer<AceKey?> {
    @Throws(IOException::class)
    override fun write(out: ObjectDataOutput, `object`: AceKey) {
        AclKeyStreamSerializer.serialize(out, `object`.aclKey)
        PrincipalStreamSerializer.serialize(out, `object`.principal)
    }

    @Throws(IOException::class)
    override fun read(`in`: ObjectDataInput): AceKey {
        val key = AclKeyStreamSerializer.deserialize(`in`)
        val principal = PrincipalStreamSerializer.deserialize(`in`)
        return AceKey(key, principal)
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ACE_KEY.ordinal
    }

    override fun destroy() {}
    override fun getClazz(): Class<AceKey> {
        return AceKey::class.java
    }

    override fun generateTestValue(): AceKey {
        return AceKey(TestDataFactory.aclKey(), TestDataFactory.userPrincipal())
    }
}