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

import com.openlattice.chronicle.serializers.AceValueStreamSerializer.Companion.serialize
import com.openlattice.chronicle.serializers.AceValueStreamSerializer.Companion.deserialize
import com.geekbeast.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import kotlin.Throws
import java.io.IOException
import com.hazelcast.nio.ObjectDataOutput
import com.hazelcast.nio.ObjectDataInput
import com.openlattice.chronicle.authorization.processors.SecurableObjectTypeUpdater
import com.openlattice.chronicle.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class SecurableObjectTypeUpdaterStreamSerializer : SelfRegisteringStreamSerializer<SecurableObjectTypeUpdater?> {
    override fun getClazz(): Class<SecurableObjectTypeUpdater> {
        return SecurableObjectTypeUpdater::class.java
    }

    @Throws(IOException::class)
    override fun write(out: ObjectDataOutput, `object`: SecurableObjectTypeUpdater) {
        serialize(out, `object`.securableObjectType)
    }

    @Throws(IOException::class)
    override fun read(`in`: ObjectDataInput): SecurableObjectTypeUpdater {
        return SecurableObjectTypeUpdater(
            deserialize(`in`)
        )
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.SECURABLE_OBJECT_TYPE_UPDATE.ordinal
    }

    override fun destroy() {}
}