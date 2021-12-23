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

import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.collect.Lists
import com.google.common.collect.Maps
import com.google.common.collect.Sets
import com.openlattice.chronicle.authorization.*
import com.openlattice.chronicle.mapstores.ids.Range
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ACL_KEY
import com.openlattice.chronicle.storage.PostgresColumns.Companion.DESCRIPTION
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
import com.openlattice.postgres.PostgresArrays
import org.slf4j.LoggerFactory
import java.sql.ResultSet
import java.sql.SQLException
import java.time.OffsetDateTime
import java.util.*
import java.util.function.Supplier
import java.util.stream.Collectors

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
            return rs.getObject(ID.getName(), UUID::class.java)
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
            return rs.getString(URL.getName())
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
        fun linkedHashSetUUID(rs: ResultSet, colName: String?): LinkedHashSet<UUID> {
            //Curious what happens if a null slips in here
            return linkedSetOf(*(rs.getArray(colName).array as Array<UUID>))
        }

        @Throws(SQLException::class)
        fun key(rs: ResultSet): LinkedHashSet<UUID> {
            return linkedHashSetUUID(rs, KEY.getName())
        }

        @Throws(SQLException::class)
        fun properties(rs: ResultSet): LinkedHashSet<UUID> {
            return linkedHashSetUUID(rs, PROPERTIES.getName())
        }

        @Throws(SQLException::class)
        fun category(rs: ResultSet): SecurableObjectType {
            return SecurableObjectType.valueOf(rs.getString(CATEGORY.getName()))
        }

        @Throws(SQLException::class)
        fun shards(rs: ResultSet): Int {
            return rs.getInt(SHARDS.getName())
        }

        @Throws(SQLException::class)
        fun contacts(rs: ResultSet): Set<String> {
            return Sets.newHashSet(*rs.getArray(CONTACTS.getName()).getArray() as Array<String?>)
        }

        @Throws(SQLException::class)
        fun show(rs: ResultSet): Boolean {
            return rs.getBoolean(SHOW.getName())
        }

        @Throws(SQLException::class)
        fun members(rs: ResultSet): java.util.LinkedHashSet<String> {
            return Arrays.stream(rs.getArray(MEMBERS.getName()).getArray() as Array<String>)
                    .collect(
                            Collectors
                                    .toCollection(
                                            Supplier { LinkedHashSet() })
                    )
        }

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

        @Throws(SQLException::class)
        fun dataExpiration(rs: ResultSet): DataExpiration? {
            val expirationBase: ExpirationBase = expirationBase(rs)
                    ?: return null
            val timeToExpiration = timeToExpiration(rs)
            val deleteType: DeleteType? = deleteType(rs)
            val startDateProperty: UUID = startDateProperty(rs)
            return DataExpiration(
                    timeToExpiration,
                    expirationBase,
                    deleteType,
                    Optional.ofNullable(startDateProperty)
            )
        }

        @Throws(SQLException::class)
        fun timeToExpiration(rs: ResultSet): Long {
            return rs.getLong(TIME_TO_EXPIRATION_FIELD)
        }

        @Throws(SQLException::class)
        fun expirationBase(rs: ResultSet): ExpirationBase? {
            val expirationFlag: String = rs.getString(EXPIRATION_BASE_FLAG_FIELD)
            return if (expirationFlag != null) {
                ExpirationBase.valueOf(expirationFlag)
            } else null
        }

        @Throws(SQLException::class)
        fun deleteType(rs: ResultSet): DeleteType? {
            val deleteType: String = rs.getString(EXPIRATION_DELETE_FLAG_FIELD)
            return if (deleteType != null) {
                DeleteType.valueOf(deleteType)
            } else null
        }

        @Throws(SQLException::class)
        fun startDateProperty(rs: ResultSet): UUID {
            return rs.getObject<UUID>(EXPIRATION_START_ID_FIELD, UUID::class.java)
        }

        @Throws(SQLException::class, IOException::class)
        fun roles(rs: ResultSet): Map<UUID, AclKey> {
            return mapper.readValue(rs.getString(PostgresColumn.ROLES.getName()), rolesTypeRef)
        }

        @Throws(SQLException::class)
        fun count(rs: ResultSet): Long {
            return rs.getLong(COUNT)
        }

        @Throws(SQLException::class)
        fun expirationDate(rs: ResultSet): OffsetDateTime {
            return rs.getObject<OffsetDateTime>(EXPIRATION_DATE_FIELD, OffsetDateTime::class.java)
        }

        @Throws(SQLException::class)
        fun lastRead(rs: ResultSet): OffsetDateTime {
            return rs.getObject<OffsetDateTime>(LAST_READ_FIELD, OffsetDateTime::class.java)
        }

        @Throws(SQLException::class)
        fun exists(rs: ResultSet): Boolean {
            return rs.getBoolean("exists")
        }

        @Throws(SQLException::class)
        fun entitySetCollection(rs: ResultSet): EntitySetCollection {
            val id: UUID = id(rs)
            val name = name(rs)
            val title = title(rs)
            val description = Optional.ofNullable(description(rs))
            val entityTypeCollectionId: UUID = entityTypeCollectionId(rs)
            val contacts = contacts(rs)
            val organizationId: UUID = organizationId(rs)
            return EntitySetCollection(
                    id,
                    name,
                    title,
                    description,
                    entityTypeCollectionId,
                    Maps.newHashMap(),
                    contacts,
                    organizationId
            )
        }

        @Throws(SQLException::class)
        fun collectionTemplateKey(rs: ResultSet): CollectionTemplateKey {
            val entitySetCollectionId: UUID = entitySetCollectionId(rs)
            val templateTypeId: UUID = templateTypeid(rs)
            return CollectionTemplateKey(entitySetCollectionId, templateTypeId)
        }

        @Throws(SQLException::class)
        fun externalTable(rs: ResultSet): ExternalTable {
            val id: UUID = id(rs)
            val name = name(rs)
            val title = title(rs)
            val description = Optional.ofNullable(
                    description(
                            rs
                    )
            )
            val organizationId: UUID = organizationId(rs)
            val oid: Long = oid(rs)
            val schema: String = schema(rs)
            return ExternalTable(id, name, title, description, organizationId, oid, schema)
        }

        @Throws(SQLException::class)
        fun externalColumn(rs: ResultSet): ExternalColumn {
            val id: UUID = id(rs)
            val name = name(rs)
            val title = title(rs)
            val description = Optional.ofNullable(
                    description(
                            rs
                    )
            )
            val tableId: UUID = tableId(rs)
            val organizationId: UUID = organizationId(rs)
            val dataType: PostgresDatatype = sqlDataType(rs)
            val isPrimaryKey: Boolean = rs.getBoolean(IS_PRIMARY_KEY.getName())
            val ordinalPosition = ordinalPosition(rs)
            return ExternalColumn(
                    id,
                    name,
                    title,
                    description,
                    tableId,
                    organizationId,
                    dataType,
                    isPrimaryKey,
                    ordinalPosition
            )
        }

        @Throws(SQLException::class)
        fun columnName(rs: ResultSet): String {
            return rs.getString(COLUMN_NAME.getName())
        }

        @Throws(SQLException::class)
        fun columnNames(rs: ResultSet): List<String> {
            return Lists.newArrayList(*PostgresArrays.getTextArray(rs, COLUMN_NAMES_FIELD))
        }

        @Throws(SQLException::class)
        fun sqlDataType(rs: ResultSet): PostgresDatatype {
            val dataType: String = rs.getString(DATATYPE.getName()).toUpperCase()
            return PostgresDatatype.getEnum(dataType)
        }

        @Throws(SQLException::class)
        fun ordinalPosition(rs: ResultSet): Int {
            return rs.getInt(ORDINAL_POSITION.getName())
        }

        @Throws(SQLException::class)
        fun constraintType(rs: ResultSet): String {
            return rs.getString(CONSTRAINT_TYPE.getName())
        }

        @Throws(SQLException::class)
        fun privilegeType(rs: ResultSet): String {
            return rs.getString(PRIVILEGE_TYPE.getName())
        }

        @Throws(SQLException::class)
        fun user(rs: ResultSet): String {
            return rs.getString(USER.getName())
        }

        @Throws(SQLException::class)
        fun originId(rs: ResultSet): UUID {
            return rs.getObject(ORIGIN_ID.getName(), UUID::class.java)
        }

        @Throws(SQLException::class)
        fun username(rs: ResultSet): String {
            return rs.getString(USERNAME.getName())
        }

        @Throws(SQLException::class)
        fun permission(rs: ResultSet): Permission {
            return Permission.valueOf(rs.getString(PERMISSION.getName()))
        }

        @Throws(SQLException::class, IOException::class)
        fun securableObjectMetadata(rs: ResultSet): SecurableObjectMetadata {
            return mapper.readValue(rs.getString(METADATA.getName()), SecurableObjectMetadata::class.java)
        }
    }
}