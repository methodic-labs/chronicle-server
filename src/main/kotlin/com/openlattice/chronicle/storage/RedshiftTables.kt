package com.openlattice.chronicle.storage

import com.openlattice.chronicle.constants.EdmConstants.*
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.STUDY_ID
import com.openlattice.postgres.PostgresColumnDefinition
import com.openlattice.postgres.PostgresDatatype
import com.openlattice.postgres.PostgresTableDefinition

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class RedshiftTables {
    companion object {


        val CHRONICLE_USAGE_DATA = PostgresTableDefinition("chronicle_usage_data")
            .addColumns(ORGANIZATION_ID, STUDY_ID)
            .primaryKey

    }


}

class RedshiftColumns {
    companion object {
        val ORGANIZATION_ID_FIELD = "organization_id"
        val ORGANIZATION_ID = PostgresColumnDefinition(ORGANIZATION_ID_FIELD, PostgresDatatype.TEXT).notNull()
        val STUDY_ID_FIELD = "study_id"
        val STUDY_ID = PostgresColumnDefinition(STUDY_ID_FIELD, PostgresDatatype.TEXT).notNull()

        val COMPLETED = PostgresColumnDefinition(COMPLETED_DATE_TIME_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        val DATE_LOGGED = PostgresColumnDefinition(DATE_LOGGED_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        val DATE = PostgresColumnDefinition(DATE_TIME_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        val END_DATE_TIME = PostgresColumnDefinition(END_DATE_TIME_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        val FULL_NAME = PostgresColumnDefinition(FULL_NAME_FQN.name, PostgresDatatype.TEXT)
        val MODEL = PostgresColumnDefinition(MODEL_FQN.name, PostgresDatatype.TEXT)
        val OL_ID = PostgresColumnDefinition(OL_ID_FQN.name, PostgresDatatype.TEXT)
        val PERSON_ID = PostgresColumnDefinition(PERSON_ID_FQN.name, PostgresDatatype.TEXT)
        val RECORDED_DATE_TIME = PostgresColumnDefinition(RECORDED_DATE_TIME_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        val RECORD_TYPE = PostgresColumnDefinition(RECORD_TYPE_FQN.name, PostgresDatatype.TEXT)
        val START_DATE_TIME = PostgresColumnDefinition(START_DATE_TIME_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        val STATUS = PostgresColumnDefinition(STATUS_FQN.name, PostgresDatatype.TEXT)
        val STRING_ID = PostgresColumnDefinition(STRING_ID_FQN.name, PostgresDatatype.TEXT)
        val TIMEZONE = PostgresColumnDefinition(TIMEZONE_FQN.name, PostgresDatatype.TEXT)
        val TITLE = PostgresColumnDefinition(TITLE_FQN.name, PostgresDatatype.TEXT)
        val VALUES = PostgresColumnDefinition(VALUES_FQN.name, PostgresDatatype.TEXT)
        val VERSION = PostgresColumnDefinition(VERSION_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        val DURATION = PostgresColumnDefinition(DURATION_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        val USER = PostgresColumnDefinition(USER_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        val NEW_APP = PostgresColumnDefinition(NEW_APP_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        val GENERAL_END_TIME = PostgresColumnDefinition(GENERAL_END_TIME_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        val NEW_PERIOD = PostgresColumnDefinition(NEW_PERIOD_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        val WARNING = PostgresColumnDefinition(WARNING_FQN.name, PostgresDatatype.TIMESTAMPTZ)
    }
}