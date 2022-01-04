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
        val DATE_OF_BIRTH = PostgresColumnDefinition( "dob", PostgresDatatype.DATE )
        val TUD_ID = PostgresColumnDefinition("tud_id", PostgresDatatype.UUID).notNull()
        val SUBMISSION_DATE = PostgresColumnDefinition("submission_date", PostgresDatatype.DATE)
        val TUD_DATA = PostgresColumnDefinition("tud_data", PostgresDatatype.JSONB)
    }
}
