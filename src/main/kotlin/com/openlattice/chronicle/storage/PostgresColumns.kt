package com.openlattice.chronicle.storage

import com.geekbeast.postgres.PostgresColumnDefinition
import com.geekbeast.postgres.PostgresDatatype
import com.geekbeast.postgres.PostgresTableDefinition
import com.geekbeast.postgres.PostgresTables
import java.lang.reflect.Field
import java.lang.reflect.Modifier
import java.util.*
import java.util.stream.Stream

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresColumns {
    companion object {
        @JvmField val ACL_KEY = PostgresColumnDefinition("acl_key", PostgresDatatype.UUID_ARRAY)=
        @JvmField val APP_USERS = PostgresColumnDefinition("users", PostgresDatatype.TEXT_ARRAY)
        @JvmField val BASE = PostgresColumnDefinition("base", PostgresDatatype.BIGINT).notNull()
        @JvmField val CANDIDATE_ID = PostgresColumnDefinition("candidate_id", PostgresDatatype.UUID).notNull()
        @JvmField val CATEGORY = PostgresColumnDefinition("category", PostgresDatatype.TEXT).notNull()
        @JvmField val CONTACT = PostgresColumnDefinition("contact", PostgresDatatype.TEXT)
        @JvmField val CREATED_AT = PostgresColumnDefinition("created_at", PostgresDatatype.TIMESTAMPTZ).notNull().withDefault("now()")
        @JvmField val DATE_OF_BIRTH = PostgresColumnDefinition("dob", PostgresDatatype.DATE)
        @JvmField val DESCRIPTION = PostgresColumnDefinition("description", PostgresDatatype.TEXT)
        @JvmField val DELETED_ROWS = PostgresColumnDefinition("deleted_rows", PostgresDatatype.BIGINT).notNull()
        @JvmField val DEVICE_ID = PostgresColumnDefinition("device_id", PostgresDatatype.UUID).notNull()
        @JvmField val EMAIL = PostgresColumnDefinition("email", PostgresDatatype.TEXT).unique()
        @JvmField val ENDED_AT = PostgresColumnDefinition("ended_at", PostgresDatatype.TIMESTAMPTZ).notNull().withDefault("'infinity'")
        @JvmField val EXPIRATION = PostgresColumnDefinition("expiration", PostgresDatatype.BIGINT)
        @JvmField val EXPIRATION_DATE = PostgresColumnDefinition("expiration_date", PostgresDatatype.TIMESTAMPTZ).withDefault("'infinity'").notNull()
        @JvmField val FIRST_NAME = PostgresColumnDefinition("first_name", PostgresDatatype.TEXT)
        @JvmField val LAST_NAME = PostgresColumnDefinition("last_name", PostgresDatatype.TEXT)
        @JvmField val LAT = PostgresColumnDefinition("lat", PostgresDatatype.DOUBLE)
        @JvmField val LON = PostgresColumnDefinition("lon", PostgresDatatype.DOUBLE)
        @JvmField val LSB = PostgresColumnDefinition("lsb", PostgresDatatype.BIGINT).notNull()
        @JvmField val MSB = PostgresColumnDefinition("msb", PostgresDatatype.BIGINT).notNull()
        @JvmField val NAME = PostgresColumnDefinition("name", PostgresDatatype.TEXT)
        @JvmField val NOTIFICATIONS_ENABLED = PostgresColumnDefinition("notifications_enabled", PostgresDatatype.BOOLEAN)
        @JvmField val ORGANIZATION_ID = PostgresColumnDefinition("organization_id", PostgresDatatype.UUID).notNull()
        @JvmField val ORGANIZATION_IDS = PostgresColumnDefinition("organization_ids", PostgresDatatype.UUID_ARRAY).notNull()
        @JvmField val PARTICIPANT_ID = PostgresColumnDefinition("participant_id", PostgresDatatype.TEXT).notNull()
        @JvmField val PARTICIPATION_STATUS = PostgresColumnDefinition("participation_status", PostgresDatatype.TEXT).notNull()
        @JvmField val PARTITION_INDEX = PostgresColumnDefinition("partition_index", PostgresDatatype.BIGINT).notNull()
        @JvmField val PERMISSION = PostgresColumnDefinition("permission", PostgresDatatype.TEXT)
        @JvmField val PERMISSIONS = PostgresColumnDefinition("permissions", PostgresDatatype.TEXT_ARRAY)
        @JvmField val PHONE_NUMBER = PostgresColumnDefinition("phone_number", PostgresDatatype.TEXT).unique()
        @JvmField val PRINCIPAL_ID = PostgresColumnDefinition("principal_id", PostgresDatatype.TEXT)
        @JvmField val PRINCIPAL_OF_ACL_KEY = PostgresColumnDefinition("principal_of_acl_key", PostgresDatatype.UUID_ARRAY)
        @JvmField val PRINCIPAL_TYPE = PostgresColumnDefinition("principal_type", PostgresDatatype.TEXT)
        @JvmField val SCOPE = PostgresColumnDefinition("scope", PostgresDatatype.TEXT).notNull()
        @JvmField val SECURABLE_OBJECT_ID = PostgresColumnDefinition("id", PostgresDatatype.UUID).unique().notNull()
        @JvmField val SECURABLE_OBJECT_NAME = PostgresColumnDefinition("name", PostgresDatatype.TEXT).notNull().unique()
        @JvmField val SECURABLE_OBJECT_TYPE = PostgresColumnDefinition("securable_object_type", PostgresDatatype.TEXT).notNull()
        @JvmField val SECURABLE_PRINCIPAL_ID = PostgresColumnDefinition("securable_principal_id", PostgresDatatype.UUID).notNull()
        @JvmField val SETTINGS = PostgresColumnDefinition("settings", PostgresDatatype.JSONB).withDefault("'{}'::jsonb")
        @JvmField val SOURCE_DEVICE = PostgresColumnDefinition("source_device", PostgresDatatype.JSONB).notNull()
        @JvmField val SOURCE_DEVICE_ID = PostgresColumnDefinition("source_device_id", PostgresDatatype.TEXT).notNull()
        @JvmField val STARTED_AT = PostgresColumnDefinition("started_at", PostgresDatatype.TIMESTAMPTZ).notNull().withDefault("now()")
        @JvmField val STORAGE = PostgresColumnDefinition("storage", PostgresDatatype.TEXT).notNull().withDefault("'default'")
        @JvmField val STUDY_GROUP = PostgresColumnDefinition("study_group", PostgresDatatype.TEXT)
        @JvmField val STUDY_ID = PostgresColumnDefinition("study_id", PostgresDatatype.UUID).notNull()
        @JvmField val STUDY_VERSION = PostgresColumnDefinition("study_version", PostgresDatatype.TEXT)
        @JvmField val SUBMISSION = PostgresColumnDefinition("submission", PostgresDatatype.JSONB)
        @JvmField val SUBMISSION_DATE = PostgresColumnDefinition("submission_date", PostgresDatatype.TIMESTAMPTZ).notNull().withDefault("NOW()")
        @JvmField val SUBMISSION_ID = PostgresColumnDefinition("submission_id", PostgresDatatype.UUID).notNull()
        @JvmField val TITLE = PostgresColumnDefinition("title", PostgresDatatype.TEXT)
        @JvmField val UPDATED_AT = PostgresColumnDefinition("updated_at", PostgresDatatype.TIMESTAMPTZ).notNull().withDefault("now()")
        @JvmField val URL = PostgresColumnDefinition("url", PostgresDatatype.TEXT)
        @JvmField val USER_DATA = PostgresColumnDefinition("user_data", PostgresDatatype.JSONB)
        @JvmField val USER_ID = PostgresColumnDefinition("user_id", PostgresDatatype.TEXT).notNull()
        @JvmField val JOB_ID = PostgresColumnDefinition("job_id", PostgresDatatype.UUID).notNull()
        @JvmField val STATUS = PostgresColumnDefinition("status", PostgresDatatype.TEXT)
        @JvmField val JOB_DEFINITION = PostgresColumnDefinition("definition", PostgresDatatype.JSONB).withDefault("'{}'::jsonb")
        @JvmField val MESSAGE = PostgresColumnDefinition("message", PostgresDatatype.TEXT)


        val columnTypes : Map<String, PostgresDatatype> = postgresColumns().associate { it.name to it.datatype }

        fun postgresColumns(): List<PostgresColumnDefinition> {
            return (PostgresColumns::class.java.fields.asList() + PostgresColumns::class.java.declaredFields)
                .filter { field: Field -> (Modifier.isStatic(field.modifiers) && Modifier.isFinal(field.modifiers)) }
                .filter { field: Field -> PostgresColumnDefinition::class.java.isAssignableFrom(field.type) }
                .mapNotNull { field: Field ->
                    try {
                        return@mapNotNull field[null] as PostgresColumnDefinition
                    } catch (e: IllegalAccessException) {
                        return@mapNotNull null
                    }
                }
        }
    }
}
