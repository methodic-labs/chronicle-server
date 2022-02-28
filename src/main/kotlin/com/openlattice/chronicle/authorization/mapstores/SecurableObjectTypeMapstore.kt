package com.openlattice.chronicle.authorization.mapstores

import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.PostgresColumnDefinition
import com.geekbeast.postgres.mapstores.AbstractBasePostgresMapstore
import com.hazelcast.config.IndexConfig
import com.hazelcast.config.IndexType
import com.hazelcast.config.MapConfig
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.SecurableObjectType
import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.storage.ChroniclePostgresTables
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ACL_KEY
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SECURABLE_OBJECT_TYPE
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.util.*

class SecurableObjectTypeMapstore(hds: HikariDataSource?) : AbstractBasePostgresMapstore<AclKey, SecurableObjectType>(
    HazelcastMap.SECURABLE_OBJECT_TYPES,
    ChroniclePostgresTables.SECURABLE_OBJECTS,
    hds
) {
    companion object {
        const val ACL_KEY_INDEX = "__key.index"
        const val SECURABLE_OBJECT_TYPE_INDEX = "this"
    }

    override fun getInsertColumns(): MutableList<PostgresColumnDefinition> = mutableListOf(ACL_KEY, SECURABLE_OBJECT_TYPE)


    @Throws(SQLException::class)
    override fun bind(
        ps: PreparedStatement, key: AclKey, value: SecurableObjectType
    ) {
        bind(ps, key, 1)
        ps.setString(2, value.name)
        ps.setString(3, value.name)
    }

    @Throws(SQLException::class)
    override fun bind(ps: PreparedStatement, key: AclKey, parameterIndex: Int): Int {
        var pIndex = parameterIndex
        ps.setArray(pIndex++, PostgresArrays.createUuidArray(ps.connection, key))
        return pIndex
    }

    @Throws(SQLException::class)
    override fun mapToValue(rs: ResultSet): SecurableObjectType = ResultSetAdapters.securableObjectType(rs)

    @Throws(SQLException::class)
    override fun mapToKey(rs: ResultSet): AclKey {
        return ResultSetAdapters.aclKey(rs)
    }

    override fun getMapConfig() : MapConfig {
        return super.getMapConfig()
            .addIndexConfig(IndexConfig(IndexType.HASH, ACL_KEY_INDEX))
            .addIndexConfig(IndexConfig(IndexType.HASH, SECURABLE_OBJECT_TYPE_INDEX))
    }

    override fun generateTestKey(): AclKey {
        return AclKey(UUID.randomUUID(), UUID.randomUUID())
    }

    override fun generateTestValue(): SecurableObjectType {
        return SecurableObjectType.Study
    }
}