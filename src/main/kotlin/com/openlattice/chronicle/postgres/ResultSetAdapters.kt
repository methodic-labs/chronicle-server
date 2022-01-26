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
package com.openlattice.chronicle.postgres

import com.geekbeast.mappers.mappers.ObjectMappers
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.openlattice.chronicle.authorization.*
import com.openlattice.chronicle.mapstores.ids.Range
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ACL_KEY
import com.openlattice.chronicle.storage.PostgresColumns.Companion.CATEGORY
import com.openlattice.chronicle.storage.PostgresColumns.Companion.DESCRIPTION
import com.openlattice.chronicle.storage.PostgresColumns.Companion.EXPIRATION_DATE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.LSB
import com.openlattice.chronicle.storage.PostgresColumns.Companion.MSB
import com.openlattice.chronicle.storage.PostgresColumns.Companion.NAME
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PARTITION_INDEX
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PERMISSIONS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PRINCIPAL_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PRINCIPAL_OF_ACL_KEY
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PRINCIPAL_TYPE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SECURABLE_OBJECT_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SECURABLE_OBJECT_NAME
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SECURABLE_OBJECT_TYPE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.TITLE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.URL
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.USERNAME
import com.geekbeast.postgres.PostgresArrays
import com.openlattice.chronicle.organizations.Organization
import com.openlattice.chronicle.organizations.OrganizationPrincipal
import com.openlattice.chronicle.storage.PostgresColumns.Companion.CONTACT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.CREATED_AT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ENDED_AT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.LAT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.LON
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ORGANIZATION_IDS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SETTINGS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STARTED_AT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_GROUP
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_VERSION
import com.openlattice.chronicle.storage.PostgresColumns.Companion.UPDATED_AT
import com.openlattice.chronicle.study.Study
import org.slf4j.LoggerFactory
import java.sql.ResultSet
import java.sql.SQLException
import java.time.OffsetDateTime
import java.util.*

/**
 * Use for reading count field when performing an aggregation.
 */
const val COUNT = "count"

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class ResultSetAdapters {
    companion object {
        private val logger = LoggerFactory.getLogger(ResultSetAdapters::class.java)
        private val DECODER = Base64.getMimeDecoder()
        private val mapper: ObjectMapper = ObjectMappers.newJsonMapper()
        private val rolesTypeRef: TypeReference<Map<UUID, AclKey>> = object : TypeReference<Map<UUID, AclKey>>() {}

        @JvmStatic
        @Throws(SQLException::class)
        fun range(rs: ResultSet): Range {
            val base: Long = rs.getLong(PARTITION_INDEX.name) shl 48
            val msb: Long = rs.getLong(MSB.name)
            val lsb: Long = rs.getLong(LSB.name)
            return Range(base, msb, lsb)
        }

        @Throws(SQLException::class)
        fun principalOfAclKey(rs: ResultSet): AclKey {
            val arr: Array<UUID> = try {
                checkNotNull(PostgresArrays.getUuidArray(rs, PRINCIPAL_OF_ACL_KEY.name)) {
                    "Principal of acl key cannot be null."
                }
            } catch (e: ClassCastException) {
                logger.error("Unable to read principal of acl key of acl key: {}", aclKey(rs))
                throw IllegalStateException("Unable to read principal of acl key", e)
            }
            return AclKey(*arr)
        }

        @Throws(SQLException::class)
        fun securablePrincipal(rs: ResultSet): SecurablePrincipal {
            val principal: Principal = principal(rs)
            val aclKey = aclKey(rs)
            val title = title(rs)
            val description = description(rs)
            return when (principal.type) {
                PrincipalType.ROLE -> {
                    val id: UUID = aclKey[1]
                    val organizationId: UUID = aclKey[0]
                    Role(Optional.of(id), organizationId, principal, title, Optional.of(description))
                }
                else -> SecurablePrincipal(aclKey, principal, title, Optional.of(description))
            }
        }

        @Throws(SQLException::class)
        fun permissions(rs: ResultSet): EnumSet<Permission> {
            val pStrArray: Array<String> = PostgresArrays.getTextArray(rs, PERMISSIONS.name)
            val permissions: EnumSet<Permission> = EnumSet.noneOf(Permission::class.java)
            pStrArray.forEach { permissions.add(Permission.valueOf(it)) }
            return permissions
        }

        @Throws(SQLException::class)
        fun id(rs: ResultSet): UUID {
            return UUID.fromString(rs.getString(ID.name))
        }

        @Throws(SQLException::class)
        fun name(rs: ResultSet): String {
            return rs.getString(NAME.name)
        }

        @Throws(SQLException::class)
        fun title(rs: ResultSet): String {
            return rs.getString(TITLE.name)
        }

        @Throws(SQLException::class)
        fun description(rs: ResultSet): String {
            return rs.getString(DESCRIPTION.name)
        }

        @Throws(SQLException::class)
        fun url(rs: ResultSet): String {
            return rs.getString(URL.name)
        }

        @Throws(SQLException::class)
        fun principal(rs: ResultSet): Principal {
            val principalType: PrincipalType = PrincipalType.valueOf(rs.getString(PRINCIPAL_TYPE.name))
            val principalId: String = rs.getString(PRINCIPAL_ID.name)
            return Principal(principalType, principalId)
        }

        @Throws(SQLException::class)
        fun aclKey(rs: ResultSet): AclKey {
            return AclKey(*PostgresArrays.getUuidArray(rs, ACL_KEY.name)!!)
        }

        @Throws(SQLException::class)
        fun aceKey(rs: ResultSet): AceKey {
            val aclKey = aclKey(rs)
            val principal: Principal = principal(rs)
            return AceKey(aclKey, principal)
        }

        @Throws(SQLException::class)
        fun linkedHashSetUUID(rs: ResultSet, colName: String): LinkedHashSet<UUID> {
            return LinkedHashSet<UUID>((rs.getArray(colName).array as Array<UUID?>).filterNotNull())
        }

        @Throws(SQLException::class)
        fun category(rs: ResultSet): SecurableObjectType {
            return SecurableObjectType.valueOf(rs.getString(CATEGORY.name))
        }

//        @Throws(SQLException::class)
//        fun contacts(rs: ResultSet): Set<String> {
//            return (rs.getArray(CONTACTS.getName()).getArray() as Array<String?>).filterNotNull().toSet()
//        }
//
//        @Throws(SQLException::class)
//        fun members(rs: ResultSet): java.util.LinkedHashSet<String> {
//            return Arrays.stream(rs.getArray(MEMBERS.getName()).getArray() as Array<String>)
//                    .collect(
//                            Collectors
//                                    .toCollection(
//                                            Supplier { LinkedHashSet() })
//                    )
//        }

        @Throws(SQLException::class)
        fun securableObjectId(rs: ResultSet): UUID {
            return rs.getObject(SECURABLE_OBJECT_ID.name, UUID::class.java)
        }

        @Throws(SQLException::class)
        fun securableObjectName(rs: ResultSet): String {
            return rs.getString(SECURABLE_OBJECT_NAME.name)
        }

        @Throws(SQLException::class)
        fun organizationId(rs: ResultSet): UUID {
            return rs.getObject(ORGANIZATION_ID.name, UUID::class.java)
        }

        @Throws(SQLException::class)
        fun securableObjectType(rs: ResultSet): SecurableObjectType {
            return SecurableObjectType.valueOf(rs.getString(SECURABLE_OBJECT_TYPE.name))
        }

//        @Throws(SQLException::class, IOException::class)
//        fun roles(rs: ResultSet): Map<UUID, AclKey> {
//            return mapper.readValue(rs.getString(PostgresColumn.ROLES.getName()), rolesTypeRef)
//        }

        @Throws(SQLException::class)
        fun count(rs: ResultSet): Long {
            return rs.getLong(COUNT)
        }

        @Throws(SQLException::class)
        fun expirationDate(rs: ResultSet): OffsetDateTime {
            return rs.getObject(EXPIRATION_DATE.name, OffsetDateTime::class.java)
        }

        @Throws(SQLException::class)
        fun exists(rs: ResultSet): Boolean {
            return rs.getBoolean("exists")
        }

        @Throws(SQLException::class)
        fun username(rs: ResultSet): String {
            return rs.getString(USERNAME.name)
        }

        @Throws(SQLException::class)
        fun study(rs: ResultSet): Study {
            return Study(
                rs.getObject(STUDY_ID.name, UUID::class.java),
                rs.getString(TITLE.name),
                rs.getString(DESCRIPTION.name),
                rs.getObject(CREATED_AT.name, OffsetDateTime::class.java),
                rs.getObject(UPDATED_AT.name, OffsetDateTime::class.java),
                rs.getObject(STARTED_AT.name, OffsetDateTime::class.java),
                rs.getObject(ENDED_AT.name, OffsetDateTime::class.java),
                rs.getDouble(LAT.name),
                rs.getDouble(LON.name),
                rs.getString(STUDY_GROUP.name),
                rs.getString(STUDY_VERSION.name),
                rs.getString(CONTACT.name),
                PostgresArrays.getUuidArray(rs, ORGANIZATION_IDS.name)?.toSet() ?: setOf(),
                mapper.readValue(rs.getString(SETTINGS.name))
            )
        }

        @Throws(SQLException::class)
        fun organization(rs: ResultSet): Organization {
            return Organization(
                rs.getObject(ORGANIZATION_ID.name, UUID::class.java),
                rs.getString(TITLE.name),
                rs.getString(DESCRIPTION.name),
                mapper.readValue(rs.getString(SETTINGS.name))

            )
        }
    }
}