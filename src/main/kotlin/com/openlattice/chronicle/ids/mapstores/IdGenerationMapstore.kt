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
package com.openlattice.chronicle.ids.mapstores

import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.MapConfig
import com.hazelcast.config.MapStoreConfig
import com.hazelcast.config.MapStoreConfig.InitialLoadMode
import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.openlattice.chronicle.mapstores.ids.Range
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.ID_GENERATION
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PARTITION_INDEX
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class IdGenerationMapstore(hds: HikariDataSource) : AbstractBasePostgresMapstore<Long, Range>(
        HazelcastMap.ID_GENERATION,
        ID_GENERATION,
        hds
) {
    override fun generateTestKey(): Long {
        return 1L
    }

    override fun generateTestValue(): Range {
        return Range(generateTestKey() shl 48, 1, 2)
    }

    @Throws(SQLException::class)
    override fun bind(ps: PreparedStatement, key: Long, value: Range) {
        var parameterIndex = bind(ps, key, 1)
        ps.setLong(parameterIndex++, value.msb)
        ps.setLong(parameterIndex++, value.lsb)

        //Update clause
        ps.setLong(parameterIndex++, value.msb)
        ps.setLong(parameterIndex++, value.lsb)
    }

    override fun getMapStoreConfig(): MapStoreConfig {
        return super.getMapStoreConfig().setInitialLoadMode(InitialLoadMode.EAGER)
    }

    override fun getMapConfig(): MapConfig {
        return super
                .getMapConfig()
                .setInMemoryFormat(InMemoryFormat.OBJECT)
    }

    @Throws(SQLException::class)
    override fun bind(ps: PreparedStatement, key: Long, parameterIndex: Int): Int {
        var pIndex = parameterIndex
        ps.setLong(pIndex++, key)
        return pIndex
    }

    @Throws(SQLException::class)
    override fun mapToValue(rs: ResultSet): Range {
        return ResultSetAdapters.range(rs)
    }

    @Throws(SQLException::class)
    override fun mapToKey(rs: ResultSet): Long {
        return rs.getLong(PARTITION_INDEX.name)
    }
}