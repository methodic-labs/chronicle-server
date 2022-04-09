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
package com.openlattice.chronicle.mapstores.authorization

import com.codahale.metrics.annotation.Timed
import com.geekbeast.postgres.PostgresArrays.createTextArray
import com.geekbeast.postgres.PostgresArrays.createUuidArray
import com.geekbeast.postgres.PostgresColumnDefinition
import com.geekbeast.postgres.PostgresTableDefinition
import com.geekbeast.postgres.mapstores.AbstractBasePostgresMapstore
import com.google.common.eventbus.EventBus
import com.hazelcast.config.*
import com.hazelcast.config.MapStoreConfig.InitialLoadMode
import com.openlattice.chronicle.authorization.*
import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.PERMISSIONS
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.SECURABLE_OBJECTS
import com.openlattice.chronicle.storage.PostgresColumns
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ACL_KEY
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SECURABLE_OBJECT_TYPE
import com.openlattice.chronicle.util.tests.TestDataFactory
import com.zaxxer.hikari.HikariDataSource
import org.springframework.stereotype.Component
import java.sql.Array
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.time.OffsetDateTime
import java.util.*
import java.util.stream.Collectors

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class PermissionMapstore(
    hds: HikariDataSource,
    private val eventBus: EventBus
) : AbstractBasePostgresMapstore<AceKey, AceValue>(HazelcastMap.PERMISSIONS, PERMISSIONS, hds) {

    @Throws(SQLException::class)
    override fun bind(
        ps: PreparedStatement,
        key: AceKey,
        value: AceValue
    ) {
        bind(ps, key, 1)
        val permissions: Array = createTextArray(
            ps.connection,
            value.permissions.stream().map(Permission::name)
        )
        val expirationDate = value.expirationDate
        ps.setArray(4, permissions)
        ps.setObject(5, expirationDate)
        ps.setArray(6, permissions)
        ps.setObject(7, expirationDate)
    }

    @Throws(SQLException::class)
    override fun bind(ps: PreparedStatement, aceKey: AceKey, parameterIndex: Int): Int {
        var pIndex = parameterIndex
        val p: Principal = aceKey.principal
        ps.setArray(pIndex++, createUuidArray(ps.connection, aceKey.aclKey))
        ps.setString(pIndex++, p.type.name)
        ps.setString(pIndex++, p.id)
        return pIndex
    }

    @Timed
    @Throws(SQLException::class)
    override fun mapToValue(rs: ResultSet): AceValue {
        val permissions: EnumSet<Permission> = ResultSetAdapters.permissions(rs)
        val expirationDate: OffsetDateTime = ResultSetAdapters.expirationDate(rs)
        /*
         * There is small risk of deadlock here if all readers get stuck waiting for connection from the connection pool
         * we should keep an eye out to make sure there aren't an unusual number of TimeoutExceptions being thrown.
         */
        val objectType: SecurableObjectType = ResultSetAdapters.securableObjectType(rs)
        return AceValue(permissions, objectType, expirationDate)
    }

    @Throws(SQLException::class)
    override fun mapToKey(rs: ResultSet): AceKey {
        return ResultSetAdapters.aceKey(rs)
    }

    override fun getMapStoreConfig(): MapStoreConfig {
        return super
            .getMapStoreConfig()
            .setInitialLoadMode(InitialLoadMode.EAGER)
    }

    override fun buildSelectByKeyQuery(): String {
        return selectQuery(false)
    }

    override fun buildSelectAllKeysQuery(): String {
        return selectQuery(true)
    }

    override fun getMapConfig(): MapConfig {
        return super
            .getMapConfig()
            .setInMemoryFormat(InMemoryFormat.OBJECT)
            .addIndexConfig(IndexConfig(IndexType.HASH, ACL_KEY_INDEX))
            .addIndexConfig(IndexConfig(IndexType.HASH, PRINCIPAL_INDEX))
            .addIndexConfig(IndexConfig(IndexType.HASH, PRINCIPAL_TYPE_INDEX))
            .addIndexConfig(IndexConfig(IndexType.HASH, SECURABLE_OBJECT_TYPE_INDEX))
            .addIndexConfig(IndexConfig(IndexType.HASH, PERMISSIONS_INDEX))
            .addIndexConfig(IndexConfig(IndexType.HASH, ROOT_OBJECT_INDEX))
            .addIndexConfig(IndexConfig(IndexType.SORTED, EXPIRATION_DATE_INDEX))
            .addEntryListenerConfig(
                EntryListenerConfig(
                    PermissionMapListener(eventBus),
                    false,
                    true
                )
            )
    }

    override fun generateTestKey(): AceKey {
        return AceKey(
            AclKey(UUID.randomUUID()),
            Principal(PrincipalType.USER, TestDataFactory.randomAlphanumeric(5))
        )
    }

    override fun generateTestValue(): AceValue {
        return AceValue(
            EnumSet.of(Permission.READ, Permission.WRITE),
            SecurableObjectType.Organization
        )
    }

    private fun selectQuery(allKeys: Boolean): String {
        val selectSql = selectInnerJoinQuery()
        if (!allKeys) {
            selectSql.append(" WHERE ")
                .append(
                    keyColumns().stream()
                        .map { col: PostgresColumnDefinition -> getTableColumn(PERMISSIONS, col) }
                        .map { columnName: String -> "$columnName = ? " }
                        .collect(Collectors.joining(" and "))
                )
        }
        return selectSql.toString()
    }

    private fun selectInnerJoinQuery(): StringBuilder {
        return StringBuilder("SELECT * FROM ").append(PERMISSIONS.name)
            .append(" INNER JOIN ")
            .append(SECURABLE_OBJECTS.name).append(" ON ")
            .append(getTableColumn(PERMISSIONS, ACL_KEY)).append(" = ")
            .append(getTableColumn(SECURABLE_OBJECTS, ACL_KEY))
    }

    private fun getTableColumn(table: PostgresTableDefinition, column: PostgresColumnDefinition): String {
        return "${table.name}.${column.name}"
    }

    companion object {
        const val ACL_KEY_INDEX = "__key.aclKey.index"
        const val EXPIRATION_DATE_INDEX = "expirationDate"
        const val PERMISSIONS_INDEX = "permissions[any]"
        const val PRINCIPAL_INDEX = "__key.principal"
        const val PRINCIPAL_TYPE_INDEX = "__key.principal.type"
        const val ROOT_OBJECT_INDEX = "__key.aclKey[0]"
        const val SECURABLE_OBJECT_TYPE_INDEX = "securableObjectType"

    }
}