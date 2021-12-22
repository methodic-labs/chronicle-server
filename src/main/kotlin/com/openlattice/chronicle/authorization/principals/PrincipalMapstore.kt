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
package com.openlattice.chronicle.authorization.principals

import com.hazelcast.config.*
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.SecurablePrincipal
import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.PRINCIPALS
import com.openlattice.chronicle.util.TestDataFactory
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PrincipalMapstore(hds: HikariDataSource) : AbstractBasePostgresMapstore<AclKey, SecurablePrincipal>(
        HazelcastMap.PRINCIPALS,
        PRINCIPALS,
        hds
) {
    companion object {
        const val ACL_KEY_ROOT_INDEX = "aclKey[0]"
        const val PRINCIPAL_ID_INDEX = "id"
        const val PRINCIPAL_INDEX = "principal"
        const val PRINCIPAL_TYPE_INDEX = "principalType"
        private val TEST_ROLE = TestDataFactory.role()
    }

    override fun generateTestKey(): AclKey {
        return TEST_ROLE.aclKey
    }

    override fun generateTestValue(): SecurablePrincipal {
        return TEST_ROLE
    }

    @Throws(SQLException::class)
    override fun bind(
            ps: PreparedStatement, key: AclKey, value: SecurablePrincipal
    ) {
        bind(ps, key, 1)
        ps.setString(2, value.principalType.name)
        ps.setString(3, value.name)
        ps.setString(4, value.title)
        ps.setString(5, value.description)
        ps.setString(6, value.principalType.name)
        ps.setString(7, value.name)
        ps.setString(8, value.title)
        ps.setString(9, value.description)
    }

    @Throws(SQLException::class)
    override fun bind(ps: PreparedStatement, key: AclKey, parameterIndex: Int): Int {
        var pIndex = parameterIndex
        ps.setArray(pIndex++, PostgresArrays.createUuidArray(ps.connection, key))
        return pIndex
    }

    @Throws(SQLException::class)
    override fun mapToValue(rs: ResultSet): SecurablePrincipal {
        return ResultSetAdapters.securablePrincipal(rs)
    }

    @Throws(SQLException::class)
    override fun mapToKey(rs: ResultSet): AclKey {
        return ResultSetAdapters.aclKey(rs)
    }

    override fun getMapStoreConfig(): MapStoreConfig {
        return super.getMapStoreConfig().setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
    }

    override fun getMapConfig(): MapConfig {
        return super.getMapConfig()
                .addIndexConfig(IndexConfig(IndexType.HASH, PRINCIPAL_INDEX))
                .addIndexConfig(IndexConfig(IndexType.HASH, PRINCIPAL_ID_INDEX))
                .addIndexConfig(IndexConfig(IndexType.HASH, ACL_KEY_ROOT_INDEX))
                .addIndexConfig(IndexConfig(IndexType.HASH, PRINCIPAL_TYPE_INDEX))
                .setInMemoryFormat(InMemoryFormat.OBJECT)
    }
}