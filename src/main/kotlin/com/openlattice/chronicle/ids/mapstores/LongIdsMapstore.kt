package com.openlattice.chronicle.ids.mapstores

import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.BASE_LONG_IDS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.BASE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SCOPE
import com.geekbeast.postgres.mapstores.AbstractBasePostgresMapstore
import com.zaxxer.hikari.HikariDataSource
import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.lang3.RandomUtils
import org.springframework.stereotype.Component
import java.sql.PreparedStatement
import java.sql.ResultSet

@Component
class LongIdsMapstore(hds: HikariDataSource) : AbstractBasePostgresMapstore<String, Long>(
        HazelcastMap.LONG_IDS,
        BASE_LONG_IDS,
        hds
) {
    override fun generateTestKey(): String {
        return RandomStringUtils.random(10)
    }

    override fun generateTestValue(): Long {
        return RandomUtils.nextLong(0, Long.MAX_VALUE)
    }

    override fun bind(ps: PreparedStatement, key: String, value: Long) {
        val index = bind(ps, key)
        ps.setLong(index, value)
        //UPDATE
        ps.setLong(index+1, value)
    }

    override fun mapToKey(rs: ResultSet): String {
        return rs.getString(SCOPE.name)
    }

    override fun bind(ps: PreparedStatement, key: String, offset: Int): Int {
        ps.setString(offset, key)
        return offset+1
    }

    override fun mapToValue(rs: ResultSet): Long {
        return rs.getLong(BASE.name)
    }
}