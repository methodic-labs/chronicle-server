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
 *
 */
package com.openlattice.chronicle.serializers

import com.geekbeast.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import kotlin.Throws
import java.io.IOException
import com.hazelcast.nio.ObjectDataOutput
import com.hazelcast.nio.ObjectDataInput
import com.openlattice.chronicle.hazelcast.StreamSerializerTypeIds
import com.openlattice.chronicle.mapstores.ids.Range
import org.springframework.stereotype.Component

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class RangeStreamSerializer : SelfRegisteringStreamSerializer<Range> {
    override fun getClazz(): Class<Range> {
        return Range::class.java
    }

    @Throws(IOException::class)
    override fun write(out: ObjectDataOutput, `object`: Range) {
        out.writeLong(`object`.base)
        out.writeLong(`object`.msb)
        out.writeLong(`object`.lsb)
    }

    @Throws(IOException::class)
    override fun read(`in`: ObjectDataInput): Range {
        return Range(`in`.readLong(), `in`.readLong(), `in`.readLong())
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.RANGE.ordinal
    }

    override fun destroy() {}
}