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

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.rhizome.jobs.JobStatus
import com.openlattice.chronicle.authorization.AceKey
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.Permission
import com.openlattice.chronicle.authorization.Principal
import com.openlattice.chronicle.authorization.PrincipalType
import com.openlattice.chronicle.authorization.Role
import com.openlattice.chronicle.authorization.SecurableObjectType
import com.openlattice.chronicle.authorization.SecurablePrincipal
import com.openlattice.chronicle.candidates.Candidate
import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.jobs.ChronicleJob
import com.openlattice.chronicle.mapstores.ids.Range
import com.openlattice.chronicle.organizations.Organization
import com.openlattice.chronicle.participants.Participant
import com.openlattice.chronicle.participants.ParticipantStats
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ACL_KEY
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ACTIVE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ANDROID_DATES_COUNT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ANDROID_FIRST_DATE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ANDROID_LAST_DATE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.CANDIDATE_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.CATEGORY
import com.openlattice.chronicle.storage.PostgresColumns.Companion.COMPLETED_AT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.CONTACT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.CREATED_AT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.DATE_OF_BIRTH
import com.openlattice.chronicle.storage.PostgresColumns.Companion.DELETED_ROWS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.DESCRIPTION
import com.openlattice.chronicle.storage.PostgresColumns.Companion.DEVICE_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.EMAIL
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ENDED_AT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.EXPIRATION_DATE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.FIRST_NAME
import com.openlattice.chronicle.storage.PostgresColumns.Companion.IOS_DATES_COUNT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.IOS_FIRST_DATE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.IOS_LAST_DATE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.JOB_DEFINITION
import com.openlattice.chronicle.storage.PostgresColumns.Companion.JOB_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.LAST_NAME
import com.openlattice.chronicle.storage.PostgresColumns.Companion.LAT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.LON
import com.openlattice.chronicle.storage.PostgresColumns.Companion.LSB
import com.openlattice.chronicle.storage.PostgresColumns.Companion.MESSAGE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.MSB
import com.openlattice.chronicle.storage.PostgresColumns.Companion.NAME
import com.openlattice.chronicle.storage.PostgresColumns.Companion.NOTIFICATIONS_ENABLED
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ORGANIZATION_IDS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PARTICIPATION_STATUS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PARTITION_INDEX
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PERMISSIONS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PHONE_NUMBER
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PRINCIPAL_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PRINCIPAL_OF_ACL_KEY
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PRINCIPAL_TYPE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.QUESTIONNAIRE_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.QUESTIONS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.RECURRENCE_RULE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SECURABLE_OBJECT_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SECURABLE_OBJECT_NAME
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SECURABLE_OBJECT_TYPE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SECURABLE_PRINCIPAL_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SETTINGS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STARTED_AT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STATUS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STORAGE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_GROUP
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_VERSION
import com.openlattice.chronicle.storage.PostgresColumns.Companion.TITLE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.TUD_FIRST_DATE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.TUD_LAST_DATE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.UPDATED_AT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.URL
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APPLICATION_LABEL
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_PACKAGE_NAME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMESTAMP
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMEZONE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.USERNAME
import com.openlattice.chronicle.study.Study
import com.openlattice.chronicle.survey.AppUsage
import com.openlattice.chronicle.survey.Questionnaire
import org.dmfs.rfc5545.recur.RecurrenceRule
import org.slf4j.LoggerFactory
import java.sql.ResultSet
import java.sql.SQLException
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZoneId
import java.util.Base64
import java.util.EnumSet
import java.util.Optional
import java.util.UUID

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
        fun deviceId(rs: ResultSet): UUID = rs.getObject(DEVICE_ID.name, UUID::class.java)

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
        fun studyId(rs: ResultSet): UUID = rs.getObject(STUDY_ID.name, UUID::class.java)

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
                rs.getBoolean(NOTIFICATIONS_ENABLED.name),
                rs.getString(STORAGE.name),
                settings = mapper.readValue(rs.getString(SETTINGS.name))
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

        @Throws(SQLException::class)
        fun candidate(rs: ResultSet): Candidate {
            return Candidate(
                rs.getObject(CANDIDATE_ID.name, UUID::class.java),
                rs.getString(FIRST_NAME.name),
                rs.getString(LAST_NAME.name),
                rs.getString(NAME.name),
                rs.getObject(DATE_OF_BIRTH.name, LocalDate::class.java),
                rs.getString(EMAIL.name),
                rs.getString(PHONE_NUMBER.name)
            )
        }

        @Throws(SQLException::class)
        fun appUsage(rs: ResultSet): AppUsage {
            val timezone = rs.getString(TIMEZONE.name)
            val timestamp = rs.getObject(TIMESTAMP.name, OffsetDateTime::class.java)
            val zoneId = ZoneId.of(timezone)

            return AppUsage(
                rs.getString(APP_PACKAGE_NAME.name),
                rs.getString(APPLICATION_LABEL.name),
                timestamp.toInstant().atZone(zoneId).toOffsetDateTime(),
                users = listOf(),
                timezone
            )
        }

        @Throws(SQLException::class)
        fun participantStatus(rs: ResultSet): ParticipationStatus {
            return ParticipationStatus.valueOf(rs.getString(PARTICIPATION_STATUS.name))
        }

        @Throws(SQLException::class)
        fun candidateId(rs: ResultSet): UUID {
            return rs.getObject(CANDIDATE_ID.name, UUID::class.java)
        }

        @Throws(SQLException::class)
        fun storage(rs: ResultSet): String {
            return rs.getString(STORAGE.name)
        }

        @Throws(SQLException::class)
        fun chronicleJob(rs: ResultSet): ChronicleJob {
            return ChronicleJob(
                rs.getObject(JOB_ID.name, UUID::class.java),
                rs.getObject(SECURABLE_PRINCIPAL_ID.name, UUID::class.java),
                Principal(PrincipalType.valueOf(rs.getString(PRINCIPAL_TYPE.name)), rs.getString(PRINCIPAL_ID.name)),
                rs.getObject(CREATED_AT.name, OffsetDateTime::class.java),
                rs.getObject(UPDATED_AT.name, OffsetDateTime::class.java),
                rs.getObject(COMPLETED_AT.name, OffsetDateTime::class.java),
                JobStatus.valueOf(rs.getString(STATUS.name)),
                rs.getString(CONTACT.name),
                definition = mapper.readValue(rs.getString(JOB_DEFINITION.name)),
                rs.getString(MESSAGE.name),
                rs.getLong(DELETED_ROWS.name)
            )
        }

        @Throws(SQLException::class)
        fun participant(rs: ResultSet): Participant {
            return Participant(
                rs.getString(PARTICIPANT_ID.name),
                Candidate(id = rs.getObject(CANDIDATE_ID.name, UUID::class.java)),
                participantStatus(rs)
            )
        }

        @Throws(SQLException::class)
        fun questionnaire(rs: ResultSet): Questionnaire {
            return Questionnaire(
                rs.getObject(QUESTIONNAIRE_ID.name, UUID::class.java),
                rs.getString(TITLE.name),
                rs.getObject(CREATED_AT.name, OffsetDateTime::class.java),
                rs.getString(DESCRIPTION.name),
                rs.getBoolean(ACTIVE.name),
                mapper.readValue(rs.getString(QUESTIONS.name)),
                rs.getString(RECURRENCE_RULE.name)
            )
        }

        @Throws
        fun participantStats(rs: ResultSet): ParticipantStats {
            return ParticipantStats(
                rs.getString(PARTICIPANT_ID.name),
                rs.getObject(ANDROID_FIRST_DATE.name, OffsetDateTime::class.java),
                rs.getObject(ANDROID_LAST_DATE.name, OffsetDateTime::class.java),
                rs.getInt(ANDROID_DATES_COUNT.name),
                rs.getObject(IOS_FIRST_DATE.name, OffsetDateTime::class.java),
                rs.getObject(IOS_LAST_DATE.name, OffsetDateTime::class.java),
                rs.getInt(IOS_DATES_COUNT.name),
                rs.getObject(TUD_FIRST_DATE.name, OffsetDateTime::class.java),
                rs.getObject(TUD_LAST_DATE.name, OffsetDateTime::class.java),
                rs.getInt(IOS_DATES_COUNT.name)
            )
        }
    }
}
