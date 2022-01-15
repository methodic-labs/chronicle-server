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
package com.openlattice.chronicle.authorization.mapstores

import com.auth0.json.mgmt.users.User
import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.MapConfig
import com.hazelcast.config.MapStoreConfig
import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.USERS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.USER_DATA
import com.openlattice.chronicle.storage.PostgresColumns.Companion.USER_ID
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore
import com.zaxxer.hikari.HikariDataSource
import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.lang3.RandomUtils
import org.springframework.stereotype.Component
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException

/**
 * Implementation of persistence layer for users from auth0.
 *
 * TODO: It reads EXPIRATION unnecessarily since it is not part of the object stored in memory. Minor optimization
 * to not read this,but would require some work on abstract mapstores.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class UserMapstore(hds: HikariDataSource) : AbstractBasePostgresMapstore<String, User>(
        HazelcastMap.USERS, USERS, hds
) {
    private val mapper: ObjectMapper = ObjectMappers.newJsonMapper()

    @Throws(SQLException::class)
    override fun bind(ps: PreparedStatement, key: String, offset: Int): Int {
        ps.setString(offset, key)
        return offset + 1
    }

    override fun generateTestKey(): String {
        return RandomStringUtils.random(10)
    }

    override fun generateTestValue(): User {
        val user = User("conn")
        user.appMetadata = ImmutableMap.of<String, Any>(
                "foo", ImmutableList.of(
                "1", "2", "3"
        )
        )
        user.setClientId(RandomStringUtils.random(8))
        user.isBlocked = RandomUtils.nextBoolean()
        user.email = "foobar@openlattice"
        user.id = RandomStringUtils.random(8)
        user.familyName = "bar"
        user.givenName = "foo"
        user.name = "Foo bar"
        user.setVerifyEmail(RandomUtils.nextBoolean())
        user.isEmailVerified = RandomUtils.nextBoolean()
        user.nickname = RandomStringUtils.random(10)
        user.setPassword(RandomStringUtils.random(8))
        user.picture = RandomStringUtils.random(8)
        user.isPhoneVerified = RandomUtils.nextBoolean()
        user.setVerifyPhoneNumber(RandomUtils.nextBoolean())
        user.phoneNumber = RandomStringUtils.random(8)
        user.userMetadata = ImmutableMap.of<String, Any>(
                "bar", ImmutableList.of(
                "4", "5", "6"
        )
        )
        return user
    }

    override fun getMapConfig(): MapConfig {
        return super.getMapConfig()
                .setInMemoryFormat(InMemoryFormat.BINARY)
                .setMapStoreConfig(getMapStoreConfig())
    }

    override fun getMapStoreConfig(): MapStoreConfig {
        return super.getMapStoreConfig()
                .setImplementation(this)
                .setEnabled(true)
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
    }


    @Throws(SQLException::class)
    override fun bind(ps: PreparedStatement, key: String, value: User) {
        var offset: Int = bind(ps, key)
        try {
            ps.setString(offset++, mapper.writeValueAsString(value))
            ps.setLong(offset, System.currentTimeMillis())
        } catch (e: JsonProcessingException) {
            throw SQLException("Unable to serialize to JSONB.", e)
        }
    }

    @Throws(SQLException::class)
    override fun mapToKey(rs: ResultSet): String {
        return rs.getString(USER_ID.name)
    }

    @Throws(SQLException::class)
    override fun mapToValue(rs: ResultSet): User {
        return mapper.readValue(rs.getString(USER_DATA.name), User::class.java)
    }
}