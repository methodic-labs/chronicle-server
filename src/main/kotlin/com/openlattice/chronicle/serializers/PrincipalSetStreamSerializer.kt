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

import com.google.common.collect.Sets
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.chronicle.authorization.Principal
import com.openlattice.chronicle.authorization.PrincipalSet
import com.openlattice.chronicle.hazelcast.StreamSerializerTypeIds
import com.openlattice.chronicle.util.TestDataFactory
import com.geekbeast.hazelcast.serializers.SetStreamSerializer
import org.springframework.stereotype.Component
import java.io.IOException

@Component
class PrincipalSetStreamSerializer : SetStreamSerializer<PrincipalSet, Principal>(PrincipalSet::class.java) {

    override fun newInstanceWithExpectedSize(size: Int): PrincipalSet {
        return PrincipalSet(Sets.newHashSetWithExpectedSize(size))
    }

    @Throws(IOException::class)
    override fun readSingleElement(`in`: ObjectDataInput): Principal {
        return PrincipalStreamSerializer.deserialize(`in`)
    }

    @Throws(IOException::class)
    override fun writeSingleElement(out: ObjectDataOutput, element: Principal) {
        PrincipalStreamSerializer.serialize(out, element)
    }

    override fun generateTestValue(): PrincipalSet {
        return PrincipalSet(mutableSetOf(TestDataFactory.rolePrincipal(), TestDataFactory.userPrincipal()))
    }

    override fun getTypeId():Int = StreamSerializerTypeIds.PRINCIPAL_SET.ordinal
}