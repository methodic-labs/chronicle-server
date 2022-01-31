package com.openlattice.chronicle.storage

import com.geekbeast.postgres.PostgresColumnDefinition
import com.geekbeast.postgres.PostgresDatatype
import com.openlattice.chronicle.constants.EdmConstants
import org.apache.olingo.commons.api.edm.FullQualifiedName

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresColumns {
    companion object {
        val ACL_KEY = PostgresColumnDefinition("acl_key", PostgresDatatype.UUID_ARRAY)
        val BASE = PostgresColumnDefinition("base", PostgresDatatype.BIGINT).notNull()
        val CANDIDATE_ID = PostgresColumnDefinition("candidate_id", PostgresDatatype.UUID).notNull()
        val CATEGORY = PostgresColumnDefinition("category", PostgresDatatype.TEXT).notNull()
        val CONTACT = PostgresColumnDefinition("contact", PostgresDatatype.TEXT)
        val CREATED_AT = PostgresColumnDefinition("created_at", PostgresDatatype.TIMESTAMPTZ).notNull().withDefault("now()")
        val DATE_OF_BIRTH = PostgresColumnDefinition("dob", PostgresDatatype.DATE)
        val DESCRIPTION = PostgresColumnDefinition("description", PostgresDatatype.TEXT)
        val DEVICE_ID = PostgresColumnDefinition("device_id", PostgresDatatype.UUID).notNull()
        val EMAIL = PostgresColumnDefinition("email", PostgresDatatype.TEXT).unique()
        val ENDED_AT = PostgresColumnDefinition("ended_at", PostgresDatatype.TIMESTAMPTZ).notNull().withDefault("'infinity'")
        val EXPIRATION = PostgresColumnDefinition("expiration", PostgresDatatype.BIGINT)
        val EXPIRATION_DATE = PostgresColumnDefinition("expiration_date", PostgresDatatype.TIMESTAMPTZ).withDefault("'infinity'").notNull()
        val FIRST_NAME = PostgresColumnDefinition("first_name", PostgresDatatype.TEXT)
        val LAST_NAME = PostgresColumnDefinition("last_name", PostgresDatatype.TEXT)
        val LAT = PostgresColumnDefinition("lat", PostgresDatatype.DOUBLE)
        val LON = PostgresColumnDefinition("lon", PostgresDatatype.DOUBLE)
        val LSB = PostgresColumnDefinition("lsb", PostgresDatatype.BIGINT).notNull()
        val MSB = PostgresColumnDefinition("msb", PostgresDatatype.BIGINT).notNull()
        val NAME = PostgresColumnDefinition("name", PostgresDatatype.TEXT)
        val ORGANIZATION_ID = PostgresColumnDefinition("organization_id", PostgresDatatype.UUID).notNull()
        val ORGANIZATION_IDS = PostgresColumnDefinition("organization_ids", PostgresDatatype.UUID_ARRAY).notNull()
        val PARTICIPANT_ID = PostgresColumnDefinition("participant_id", PostgresDatatype.TEXT).notNull()
        val PARTITION_INDEX = PostgresColumnDefinition("partition_index", PostgresDatatype.BIGINT).notNull()
        val PERMISSION = PostgresColumnDefinition("permission", PostgresDatatype.TEXT)
        val PERMISSIONS = PostgresColumnDefinition("permissions", PostgresDatatype.TEXT_ARRAY)
        val PHONE_NUMBER = PostgresColumnDefinition("phone_number", PostgresDatatype.TEXT).unique()
        val PRINCIPAL_ID = PostgresColumnDefinition("principal_id", PostgresDatatype.TEXT)
        val PRINCIPAL_OF_ACL_KEY = PostgresColumnDefinition("principal_of_acl_key", PostgresDatatype.UUID_ARRAY)
        val PRINCIPAL_TYPE = PostgresColumnDefinition("principal_type", PostgresDatatype.TEXT)
        val SCOPE = PostgresColumnDefinition("scope", PostgresDatatype.TEXT).notNull()
        val SECURABLE_OBJECT_ID = PostgresColumnDefinition("id", PostgresDatatype.UUID).unique().notNull()
        val SECURABLE_OBJECT_NAME = PostgresColumnDefinition("name", PostgresDatatype.TEXT).notNull().unique()
        val SECURABLE_OBJECT_TYPE = PostgresColumnDefinition("securable_object_type", PostgresDatatype.TEXT).notNull()
        val SETTINGS = PostgresColumnDefinition("settings", PostgresDatatype.JSONB).withDefault("'{}'::jsonb")
        val SOURCE_DEVICE = PostgresColumnDefinition("source_device", PostgresDatatype.JSONB).notNull()
        val SOURCE_DEVICE_ID = PostgresColumnDefinition("source_device_id", PostgresDatatype.TEXT).notNull()
        val STARTED_AT = PostgresColumnDefinition("started_at", PostgresDatatype.TIMESTAMPTZ).notNull().withDefault("now()")
        val STUDY_GROUP = PostgresColumnDefinition("study_group", PostgresDatatype.TEXT)
        val STUDY_ID = PostgresColumnDefinition("study_id", PostgresDatatype.UUID).notNull()
        val STUDY_VERSION = PostgresColumnDefinition("study_version", PostgresDatatype.TEXT)
        val TITLE = PostgresColumnDefinition("title", PostgresDatatype.TEXT)
        val UPDATED_AT = PostgresColumnDefinition("updated_at", PostgresDatatype.TIMESTAMPTZ).notNull().withDefault("now()")
        val URL = PostgresColumnDefinition("url", PostgresDatatype.TEXT)
        val USER_DATA = PostgresColumnDefinition("user_data", PostgresDatatype.JSONB)
        val USER_ID = PostgresColumnDefinition("user_id", PostgresDatatype.TEXT).notNull()

        // app usage survey specific columns
        val APP_LABEL = PostgresColumnDefinition("app_label", PostgresDatatype.TEXT)
        val APP_PACKAGE_NAME = PostgresColumnDefinition("app_package_name", PostgresDatatype.TEXT).notNull()
        val APP_USAGE_ID = PostgresColumnDefinition("id", PostgresDatatype.UUID).notNull()
        val APP_USAGE_USERS = PostgresColumnDefinition("app_users", PostgresDatatype.TEXT_ARRAY)
        val APP_USAGE_TIMESTAMP = PostgresColumnDefinition("timestamp", PostgresDatatype.TIMESTAMPTZ).notNull()
        val APP_USAGE_DATE = PostgresColumnDefinition("date", PostgresDatatype.DATE).notNull()
        val APP_USAGE_TIMEZONE = PostgresColumnDefinition("timezone", PostgresDatatype.TEXT).notNull()

        // fqn to column mapping
        val FQNS_TO_APP_USAGE_COLUMNS: Map<FullQualifiedName, PostgresColumnDefinition> = mapOf(
                EdmConstants.STRING_ID_FQN to APP_USAGE_ID,
                EdmConstants.FULL_NAME_FQN to APP_PACKAGE_NAME,
                EdmConstants.TITLE_FQN to APP_LABEL,
                EdmConstants.USER_FQN to APP_USAGE_USERS,
                EdmConstants.DATE_LOGGED_FQN to APP_USAGE_TIMESTAMP,
                EdmConstants.TIMEZONE_FQN to APP_USAGE_TIMEZONE
        )
    }
}
