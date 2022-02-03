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

import com.geekbeast.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import com.geekbeast.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.chronicle.hazelcast.StreamSerializerTypeIds
import com.openlattice.chronicle.hazelcast.processors.storage.StudyStorageUpdate
import jodd.util.RandomString
import org.apache.commons.lang3.RandomStringUtils
import org.springframework.stereotype.Component
import java.io.IOException

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class StudyStorageUpdateStreamSerializer : TestableSelfRegisteringStreamSerializer<StudyStorageUpdate> {
    override fun getClazz(): Class<StudyStorageUpdate> {
        return StudyStorageUpdate::class.java
    }

    @Throws(IOException::class)
    override fun write(out: ObjectDataOutput, obj: StudyStorageUpdate) {
        out.writeString(obj.storage)
    }

    @Throws(IOException::class)
    override fun read(`in`: ObjectDataInput): StudyStorageUpdate {
        return StudyStorageUpdate(`in`.readString()!!)
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.STUDY_STORAGE_UPDATE.ordinal
    }

    override fun generateTestValue(): StudyStorageUpdate {
        return StudyStorageUpdate(RandomStringUtils.randomAlphanumeric(5))
    }
}