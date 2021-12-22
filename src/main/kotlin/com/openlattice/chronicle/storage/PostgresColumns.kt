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
        val DESCRIPTION = PostgresColumnDefinition("title", PostgresDatatype.TEXT)
        val SETTINGS = PostgresColumnDefinition("settings", PostgresDatatype.JSONB)
        val NAME = PostgresColumnDefinition("name", PostgresDatatype.TEXT)
        val FIRST_NAME = PostgresColumnDefinition("first_name", PostgresDatatype.TEXT)
        val LAST_NAME = PostgresColumnDefinition("last_name", PostgresDatatype.TEXT)
        val DATE_OF_BIRTH = PostgresColumnDefinition("dob", PostgresDatatype.DATE)

        val ACL_KEY = PostgresColumnDefinition("acl_key", PostgresDatatype.UUID_ARRAY)
        val PRINCIPAL_ID = PostgresColumnDefinition("principal_id", PostgresDatatype.TEXT)
        val PRINCIPAL_OF_ACL_KEY = PostgresColumnDefinition("principal_of_acl_key", PostgresDatatype.UUID_ARRAY)
        val PRINCIPAL_TYPE = PostgresColumnDefinition("principal_type", PostgresDatatype.TEXT)
        val PARTITION_INDEX = PostgresColumnDefinition("partition_index", PostgresDatatype.BIGINT).notNull()
        val MSB = PostgresColumnDefinition("msb", PostgresDatatype.BIGINT).notNull()
        val LSB = PostgresColumnDefinition("lsb", PostgresDatatype.BIGINT).notNull()

        val PERMISSIONS = PostgresColumnDefinition("permissions", PostgresDatatype.TEXT_ARRAY)
        val PERMISSION = PostgresColumnDefinition("permission", PostgresDatatype.TEXT)
        val EXPIRATION_DATE = PostgresColumnDefinition("expiration_date", PostgresDatatype.TIMESTAMPTZ)
                .withDefault("'infinity'")
                .notNull()
        val SECURABLE_OBJECTID = PostgresColumnDefinition("securable_objectid", PostgresDatatype.UUID).notNull()
        val SECURABLE_OBJECT_TYPE = PostgresColumnDefinition("securable_object_type", PostgresDatatype.TEXT).notNull()
    }
}
