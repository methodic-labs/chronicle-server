package com.openlattice.chronicle.storage

import com.openlattice.postgres.PostgresColumnDefinition
import com.openlattice.postgres.PostgresDatatype

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresColumns {
    companion object {
        val ORGANIZATION_ID = PostgresColumnDefinition("organization_id", PostgresDatatype.UUID).notNull()
        val STUDY_ID = PostgresColumnDefinition("study_id", PostgresDatatype.UUID).notNull()
        val PARTICIPANT_ID = PostgresColumnDefinition("particpant_id", PostgresDatatype.TEXT).notNull()
        val TITLE = PostgresColumnDefinition("title", PostgresDatatype.TEXT)
        val DESCRIPTION = PostgresColumnDefinition("description", PostgresDatatype.TEXT)
        val SETTINGS = PostgresColumnDefinition("settings", PostgresDatatype.JSONB).withDefault("'{}'::jsonb")
        val NAME = PostgresColumnDefinition("name", PostgresDatatype.TEXT)
        val FIRST_NAME = PostgresColumnDefinition("first_name", PostgresDatatype.TEXT)
        val LAST_NAME = PostgresColumnDefinition("last_name", PostgresDatatype.TEXT)
        val DATE_OF_BIRTH = PostgresColumnDefinition("dob", PostgresDatatype.DATE)

        val ACL_KEY = PostgresColumnDefinition("acl_key", PostgresDatatype.UUID_ARRAY)
        val PRINCIPAL_ID = PostgresColumnDefinition("principal_id", PostgresDatatype.TEXT)
        val PRINCIPAL_TYPE = PostgresColumnDefinition("principal_type", PostgresDatatype.TEXT)
        val PRINCIPAL_OF_ACL_KEY = PostgresColumnDefinition("principal_of_acl_key", PostgresDatatype.UUID_ARRAY)
        val PARTITION_INDEX = PostgresColumnDefinition("partition_index", PostgresDatatype.BIGINT).notNull()
        val MSB = PostgresColumnDefinition("msb", PostgresDatatype.BIGINT).notNull()
        val LSB = PostgresColumnDefinition("lsb", PostgresDatatype.BIGINT).notNull()

        val PERMISSIONS = PostgresColumnDefinition("permissions", PostgresDatatype.TEXT_ARRAY)
        val PERMISSION = PostgresColumnDefinition("permission", PostgresDatatype.TEXT)
        val EXPIRATION_DATE = PostgresColumnDefinition("expiration_date", PostgresDatatype.TIMESTAMPTZ)
                .withDefault("'infinity'")
                .notNull()


        val SECURABLE_OBJECT_NAME = PostgresColumnDefinition("name", PostgresDatatype.TEXT)
                .notNull()
                .unique()
        val SECURABLE_OBJECT_ID = PostgresColumnDefinition("id", PostgresDatatype.UUID)
                .unique()
                .notNull()
        val SECURABLE_OBJECT_TYPE = PostgresColumnDefinition("securable_object_type", PostgresDatatype.TEXT)
                .notNull()

        val SCOPE = PostgresColumnDefinition("scope", PostgresDatatype.TEXT).notNull()
        val BASE = PostgresColumnDefinition("base", PostgresDatatype.BIGINT).notNull()
        val URL = PostgresColumnDefinition("url", PostgresDatatype.TEXT)
        val CATEGORY = PostgresColumnDefinition("category", PostgresDatatype.TEXT).notNull()
        val EXPIRATION = PostgresColumnDefinition("expiration", PostgresDatatype.BIGINT)
        val USER_DATA = PostgresColumnDefinition("user_data", PostgresDatatype.JSONB)
        val USER_ID = PostgresColumnDefinition("user_id", PostgresDatatype.TEXT).notNull()

        val CREATED_AT = PostgresColumnDefinition("created_at", PostgresDatatype.TIMESTAMPTZ).notNull().withDefault("now()")
        val ENDED_AT = PostgresColumnDefinition("ended_at", PostgresDatatype.TIMESTAMPTZ).notNull().withDefault("'infinity'")
        val STARTED_AT = PostgresColumnDefinition("started_at", PostgresDatatype.TIMESTAMPTZ).notNull().withDefault("now()")
        val UPDATED_AT = PostgresColumnDefinition("updated_at", PostgresDatatype.TIMESTAMPTZ).notNull().withDefault("now()")
        val STUDY_GROUP = PostgresColumnDefinition("study_group", PostgresDatatype.TEXT)
        val STUDY_VERSION = PostgresColumnDefinition("study_version", PostgresDatatype.TEXT)

        //It is fine to use double for lat/lon since we won't be doing computation on these
        val LAT = PostgresColumnDefinition("lat", PostgresDatatype.DOUBLE)
        val LON = PostgresColumnDefinition("lon", PostgresDatatype.DOUBLE)

        // app usage survey specific columns
        val APP_TITLE = PostgresColumnDefinition("app_title", PostgresDatatype.TEXT)
        val APP_PACKAGE_NAME = PostgresColumnDefinition("app_package_name", PostgresDatatype.TEXT).notNull()
        val APP_USAGE_ID = PostgresColumnDefinition("id", PostgresDatatype.UUID).notNull()
        val APP_USAGE_USERS = PostgresColumnDefinition("users", PostgresDatatype.TEXT_ARRAY)
        val APP_USAGE_TIMESTAMP = PostgresColumnDefinition("timestamp", PostgresDatatype.TIMESTAMPTZ)
        val APP_USAGE_DATE = PostgresColumnDefinition("date", PostgresDatatype.DATE)
    }
}
