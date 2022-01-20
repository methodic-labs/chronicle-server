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
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.chronicle.hazelcast.StreamSerializerTypeIds
import com.openlattice.chronicle.users.Auth0SyncTask
import com.openlattice.chronicle.users.DefaultAuth0SyncTask
import com.openlattice.chronicle.users.LocalAuth0SyncTask
import org.springframework.stereotype.Component
import java.io.IOException

/**
 * Stream
 */
@Component
class Auth0SyncTaskStreamSerializer : SelfRegisteringStreamSerializer<Auth0SyncTask> {
    override fun getClazz(): Class<Auth0SyncTask> {
        return Auth0SyncTask::class.java
    }

    @Throws(IOException::class)
    override fun write(out: ObjectDataOutput, `object`: Auth0SyncTask) {
        out.writeBoolean(`object`.isLocal)
    }

    @Throws(IOException::class)
    override fun read(`in`: ObjectDataInput): Auth0SyncTask {
        return if (`in`.readBoolean()) {
            LocalAuth0SyncTask()
        } else DefaultAuth0SyncTask()
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.AUTH0_SYNC_TASK.ordinal
    }

    override fun destroy() {}
}