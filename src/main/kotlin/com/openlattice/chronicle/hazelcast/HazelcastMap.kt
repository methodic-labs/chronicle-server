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
package com.openlattice.chronicle.hazelcast

import com.auth0.json.mgmt.users.User
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.openlattice.chronicle.authorization.*
import com.openlattice.postgres.mapstores.TypedMapIdentifier
import java.util.*

class HazelcastMap<K, V> internal constructor(val name: String) : TypedMapIdentifier<K, V> {
    private val checker = instanceChecker.checkInstance(name)

    init {
        if (checker.ok()) {
            valuesCache.add(this)
        }
    }

    override fun name(): String {
        this.checker.check()
        return name
    }

    override fun toString(): String {
        return name
    }

    fun getMap(hazelcast: HazelcastInstance): IMap<K, V> {
        this.checker.check()
        return hazelcast.getMap(name)
    }

    companion object {
        private val valuesCache: MutableList<HazelcastMap<*, *>> = ArrayList()
        private val instanceChecker = UniqueInstanceManager(HazelcastMap::class.java)

        // @formatter:off

        // When adding new entries to this list, please make sure to keep it sorted and keep the name in sync

        // @formatter:off

        @JvmField val ACL_KEYS = HazelcastMap<String, UUID>("ACL_KEYS")
//        @JvmField val CODEX_LOCKS = HazelcastMap<SmsInformationKey, Long>("CODEX_LOCKS")
//        @JvmField val CODEX_MEDIA = HazelcastMap<UUID, Base64Media>("CODEX_MEDIA")
//        @JvmField val DB_CREDS = HazelcastMap<AclKey, MaterializedViewAccount>("DB_CREDS")
//        @JvmField val EXTERNAL_COLUMNS = HazelcastMap<UUID, ExternalColumn>("EXTERNAL_COLUMNS")
//        @JvmField val EXTERNAL_TABLES = HazelcastMap<UUID, ExternalTable>("EXTERNAL_TABLES")
//        @JvmField val ID_GENERATION = HazelcastMap<Long, Range>("ID_GENERATION")
//        @JvmField val INDEXING_JOBS = HazelcastMap<UUID, DelegatedUUIDSet>("INDEXING_JOBS")
//        @JvmField val INDEXING_LOCKS = HazelcastMap<UUID, Long>("INDEXING_LOCKS")
//        @JvmField val INDEXING_PROGRESS = HazelcastMap<UUID, UUID>("INDEXING_PROGRESS")
        @JvmField val LONG_IDS = HazelcastMap<String, Long>("LONG_IDS")
        @JvmField val NAMES = HazelcastMap<UUID, String>("NAMES")
//        @JvmField val OBJECT_METADATA = HazelcastMap<AclKey, SecurableObjectMetadata>("OBJECT_METADATA")
//        @JvmField val ORGANIZATION_DATABASES = HazelcastMap<UUID, OrganizationDatabase>("ORGANIZATION_DATABASES")
//        @JvmField val ORGANIZATIONS = HazelcastMap<UUID, Organization>("ORGANIZATIONS")
        @JvmField val PERMISSIONS = HazelcastMap<AceKey, AceValue>("PERMISSIONS")
        @JvmField val PRINCIPAL_TREES = HazelcastMap<AclKey, AclKeySet>("PRINCIPAL_TREES")
        @JvmField val PRINCIPALS = HazelcastMap<AclKey, SecurablePrincipal>("PRINCIPALS")
//        @JvmField val REQUESTS = HazelcastMap<AceKey, Status>("REQUESTS")
        @JvmField val RESOLVED_PRINCIPAL_TREES = HazelcastMap<String, SortedPrincipalSet>("RESOLVED_PRINCIPAL_TREES")
//        @JvmField val SCHEDULED_TASK_LOCKS = HazelcastMap<UUID, Long>("SCHEDULED_TASK_LOCKS")
//        @JvmField val SCHEDULED_TASKS = HazelcastMap<UUID, ScheduledTask>("SCHEDULED_TASKS")
        @JvmField val SECURABLE_OBJECT_TYPES = HazelcastMap<AclKey, SecurableObjectType>("SECURABLE_OBJECT_TYPES")
        @JvmField val SECURABLE_PRINCIPALS = HazelcastMap<String, SecurablePrincipal>("SECURABLE_PRINCIPALS")
//        @JvmField val SMS_INFORMATION = HazelcastMap<SmsInformationKey, SmsEntitySetInformation>("SMS_INFORMATION")
        @JvmField val USERS = HazelcastMap<String, User>("USERS")

        // @formatter:on

        @JvmStatic
        fun values(): Array<HazelcastMap<*, *>> {
            return valuesCache.toTypedArray()
        }

        @JvmStatic
        fun valueOf(name: String): HazelcastMap<*, *> {
            for (e in valuesCache) {
                if (e.name == name) {
                    return e
                }
            }
            throw IllegalArgumentException("Map with name \"$name\" not found")
        }
    }
}