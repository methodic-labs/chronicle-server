package com.openlattice.chronicle.storage

import com.openlattice.chronicle.constants.EdmConstants
import com.openlattice.postgres.PostgresColumnDefinition
import com.openlattice.postgres.PostgresDatatype
import org.apache.olingo.commons.api.edm.FullQualifiedName

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class RedshiftColumns {
    companion object {
        val APP_PACKAGE_NAME = PostgresColumnDefinition("app_package_name", PostgresDatatype.TEXT)

        val ID = PostgresColumnDefinition("id", PostgresDatatype.TEXT).notNull()
        //IMPORTANCE in client code base
        val INTERACTION_TYPE = PostgresColumnDefinition("interaction_type", PostgresDatatype.TEXT)
        val TIMESTAMP = PostgresColumnDefinition("event_timestamp", PostgresDatatype.TIMESTAMPTZ)
        val TIMEZONE = PostgresColumnDefinition("timezone", PostgresDatatype.TEXT)
        val USER = PostgresColumnDefinition("user", PostgresDatatype.TIMESTAMPTZ)
        val APPLICATION_LABEL = PostgresColumnDefinition("application_label", PostgresDatatype.TEXT)
        val START_TIME = PostgresColumnDefinition("start_time", PostgresDatatype.TIMESTAMPTZ )
        val END_TIME = PostgresColumnDefinition("end_time", PostgresDatatype.TIMESTAMPTZ )
        //Convert all duration to milliseconds for redshift compatibility
        val DURATION = PostgresColumnDefinition("duration", PostgresDatatype.BIGINT )

        val FQNS_TO_COLUMNS: Map<FullQualifiedName, PostgresColumnDefinition> = mapOf(
                EdmConstants.OL_ID_FQN to ID,
                EdmConstants.FULL_NAME_FQN to APP_PACKAGE_NAME,
                EdmConstants.RECORD_TYPE_FQN to INTERACTION_TYPE,
                EdmConstants.DATE_LOGGED_FQN to TIMESTAMP,
                EdmConstants.TIMEZONE_FQN to TIMEZONE,
                EdmConstants.USER_FQN to USER,
                EdmConstants.TITLE_FQN to APPLICATION_LABEL,
                EdmConstants.START_DATE_TIME_FQN to START_TIME,
                EdmConstants.END_DATE_TIME_FQN to END_TIME,
                EdmConstants.DURATION_FQN to DURATION,
        )

        val ORGANIZATION_ID = PostgresColumnDefinition("organization_id", PostgresDatatype.TEXT).notNull()
        val STUDY_ID = PostgresColumnDefinition("study_id", PostgresDatatype.TEXT).notNull()
        val PARTICIPANT_ID = PostgresColumnDefinition("participant_id", PostgresDatatype.TEXT).notNull()


        val COMPLETED = PostgresColumnDefinition(EdmConstants.COMPLETED_DATE_TIME_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        val DATE_LOGGED = PostgresColumnDefinition(EdmConstants.DATE_LOGGED_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        val DATE = PostgresColumnDefinition(EdmConstants.DATE_TIME_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        val END_DATE_TIME = PostgresColumnDefinition(EdmConstants.END_DATE_TIME_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        val MODEL = PostgresColumnDefinition(EdmConstants.MODEL_FQN.name, PostgresDatatype.TEXT)
        val OL_ID = PostgresColumnDefinition(EdmConstants.OL_ID_FQN.name, PostgresDatatype.TEXT)
        val PERSON_ID = PostgresColumnDefinition(EdmConstants.PERSON_ID_FQN.name, PostgresDatatype.TEXT)

        val RECORDED_DATE_TIME = PostgresColumnDefinition(EdmConstants.RECORDED_DATE_TIME_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        val START_DATE_TIME = PostgresColumnDefinition(EdmConstants.START_DATE_TIME_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        val STATUS = PostgresColumnDefinition(EdmConstants.STATUS_FQN.name, PostgresDatatype.TEXT)

        val STRING_ID = PostgresColumnDefinition(EdmConstants.STRING_ID_FQN.name, PostgresDatatype.TEXT)
        val TITLE = PostgresColumnDefinition(EdmConstants.TITLE_FQN.name, PostgresDatatype.TEXT)
        val VALUES = PostgresColumnDefinition(EdmConstants.VALUES_FQN.name, PostgresDatatype.TEXT)
        val VERSION = PostgresColumnDefinition(EdmConstants.VERSION_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        val NEW_APP = PostgresColumnDefinition(EdmConstants.NEW_APP_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        val GENERAL_END_TIME = PostgresColumnDefinition(EdmConstants.GENERAL_END_TIME_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        val NEW_PERIOD = PostgresColumnDefinition(EdmConstants.NEW_PERIOD_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        val WARNING = PostgresColumnDefinition(EdmConstants.WARNING_FQN.name, PostgresDatatype.TIMESTAMPTZ)
    }
}