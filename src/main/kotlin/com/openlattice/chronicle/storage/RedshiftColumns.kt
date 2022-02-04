package com.openlattice.chronicle.storage

import com.openlattice.chronicle.constants.EdmConstants
import com.geekbeast.postgres.PostgresColumnDefinition
import com.geekbeast.postgres.PostgresDatatype
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.lang.reflect.Field
import java.lang.reflect.Modifier

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class RedshiftColumns {
    companion object {
        val APP_PACKAGE_NAME = PostgresColumnDefinition("app_package_name", PostgresDatatype.TEXT)

        val ID = PostgresColumnDefinition("id", PostgresDatatype.TEXT_UUID).notNull()
        //IMPORTANCE in client code base
        val INTERACTION_TYPE = PostgresColumnDefinition("interaction_type", PostgresDatatype.TEXT)
        val TIMESTAMP = PostgresColumnDefinition("event_timestamp", PostgresDatatype.TIMESTAMPTZ)
        val TIMEZONE = PostgresColumnDefinition("timezone", PostgresDatatype.TEXT)
        val USERNAME = PostgresColumnDefinition("username", PostgresDatatype.TEXT)
        val APPLICATION_LABEL = PostgresColumnDefinition("application_label", PostgresDatatype.TEXT)
        val START_TIME = PostgresColumnDefinition("start_time", PostgresDatatype.TIMESTAMPTZ )
        val END_TIME = PostgresColumnDefinition("end_time", PostgresDatatype.TIMESTAMPTZ )
        //Convert all duration to milliseconds for redshift compatibility
        val DURATION = PostgresColumnDefinition("duration", PostgresDatatype.BIGINT )

        val FQNS_TO_COLUMNS: Map<FullQualifiedName, PostgresColumnDefinition> = mapOf(
//                EdmConstants.OL_ID_FQN to ID,
                EdmConstants.STRING_ID_FQN to ID,
                EdmConstants.FULL_NAME_FQN to APP_PACKAGE_NAME,
                EdmConstants.RECORD_TYPE_FQN to INTERACTION_TYPE,
                EdmConstants.DATE_LOGGED_FQN to TIMESTAMP,
                EdmConstants.TIMEZONE_FQN to TIMEZONE,
                EdmConstants.USER_FQN to USERNAME,
                EdmConstants.TITLE_FQN to APPLICATION_LABEL,
                EdmConstants.START_DATE_TIME_FQN to START_TIME,
                EdmConstants.END_DATE_TIME_FQN to END_TIME,
                EdmConstants.DURATION_FQN to DURATION,
        )

        val ORGANIZATION_ID = PostgresColumnDefinition("organization_id", PostgresDatatype.TEXT_UUID).notNull()
        val STUDY_ID = PostgresColumnDefinition("study_id", PostgresDatatype.TEXT_UUID).notNull()
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

        //AUDIT
        val SECURABLE_PRINCIPAL_ID = PostgresColumnDefinition("id", PostgresDatatype.TEXT_UUID).notNull()
        val PRINCIPAL_ID = PostgresColumnDefinition("principal_id", PostgresDatatype.TEXT_256).notNull()
        val ACL_KEY = PostgresColumnDefinition("acl_key", PostgresDatatype.TEXT_256).notNull()
        val AUDIT_EVENT_TYPE = PostgresColumnDefinition("audit_event_type", PostgresDatatype.TEXT_256).notNull()
        val DESCRIPTION = PostgresColumnDefinition("description", PostgresDatatype.TEXT_256).notNull()
        val DATA = PostgresColumnDefinition("data", PostgresDatatype.VARCHAR_MAX).notNull()

        // SENSOR_DATA
        // these columns apply to all sensor types
        val SAMPLE_ID = PostgresColumnDefinition("sample_id", PostgresDatatype.TEXT)
        val SENSOR_TYPE = PostgresColumnDefinition("sensor_type", PostgresDatatype.TEXT).notNull()
        val SAMPLE_DURATION = PostgresColumnDefinition("sample_duration", PostgresDatatype.DOUBLE).notNull() // seconds
        val DEVICE_VERSION = PostgresColumnDefinition("device_version", PostgresDatatype.TEXT).notNull()
        val DEVICE_NAME = PostgresColumnDefinition("device_name", PostgresDatatype.TEXT).notNull()
        val DEVICE_MODEL = PostgresColumnDefinition("device_model", PostgresDatatype.TEXT).notNull()
        val DEVICE_SYSTEM_NAME = PostgresColumnDefinition("device_system_name", PostgresDatatype.TEXT)

        // columns specific to deviceUsage sensor
        val TOTAL_SCREEN_WAKES = PostgresColumnDefinition("total_screen_wakes", PostgresDatatype.INTEGER)
        val TOTAL_UNLOCK_DURATION = PostgresColumnDefinition("total_unlock_duration", PostgresDatatype.DOUBLE) // seconds
        val APP_CATEGORY = PostgresColumnDefinition("app_category", PostgresDatatype.TEXT)
        val APP_USAGE_TIME = PostgresColumnDefinition("app_usage_time", PostgresDatatype.DOUBLE)
        val TEXT_INPUT_DEVICE = PostgresColumnDefinition("text_input_device", PostgresDatatype.TEXT)
        val TEXT_INPUT_DURATION = PostgresColumnDefinition("text_input_duration", PostgresDatatype.TEXT)
        val BUNDLE_IDENTIFIER = PostgresColumnDefinition("bundle_identifier", PostgresDatatype.TEXT)
        val APP_CATEGORY_WEB_DURATION = PostgresColumnDefinition("app_category_web_duration", PostgresDatatype.DOUBLE)
        val NOTIFICATION_INTERACTION_TYPE = PostgresColumnDefinition("notification_interaction_type", PostgresDatatype.TEXT)

        // columns specific to phoneUsage sensor
        val TOTAL_INCOMING_CALLS = PostgresColumnDefinition("total_incoming_calls", PostgresDatatype.INTEGER)
        val TOTAL_OUTGOING_CALLS = PostgresColumnDefinition("total_outgoing_calls", PostgresDatatype.INTEGER)
        val TOTAL_CALL_DURATION = PostgresColumnDefinition("total_call_duration", PostgresDatatype.DOUBLE)
        val TOTAL_UNIQUE_CONTACTS = PostgresColumnDefinition("total_unique_contacts", PostgresDatatype.INTEGER)

        // messagesUsage sensor columns
        val TOTAL_INCOMING_MESSAGES = PostgresColumnDefinition("total_incoming_messages", PostgresDatatype.INTEGER)
        val TOTAL_OUTGOING_MESSAGES = PostgresColumnDefinition("total_outgoing_messages", PostgresDatatype.INTEGER)

        // keyboard metrics sensor columns
        val KEYBOARD_VERSION = PostgresColumnDefinition("keyboard_version", PostgresDatatype.TEXT)
        val KEYBOARD_IDENTIFIER = PostgresColumnDefinition("keyboard_identifier", PostgresDatatype.TEXT)
        val TOTAL_WORDS = PostgresColumnDefinition("total_words", PostgresDatatype.INTEGER)
        val TOTAL_ALTERED_WORDS = PostgresColumnDefinition("total_altered_words", PostgresDatatype.INTEGER)
        val TOTAL_TAPS = PostgresColumnDefinition("total_taps", PostgresDatatype.INTEGER)
        val TOTAL_DRAGS = PostgresColumnDefinition("total_drags", PostgresDatatype.INTEGER)
        val TOTAL_DELETES = PostgresColumnDefinition("total_deletes", PostgresDatatype.INTEGER)
        val TOTAL_EMOJIS = PostgresColumnDefinition("total_emojis", PostgresDatatype.INTEGER)
        val TOTAL_PATHS = PostgresColumnDefinition("total_paths", PostgresDatatype.INTEGER)
        val TOTAL_PATH_TIME = PostgresColumnDefinition("total_path_time", PostgresDatatype.DOUBLE)
        val TOTAL_PATH_LENGTH = PostgresColumnDefinition("total_path_length", PostgresDatatype.INTEGER)
        val TOTAL_AUTO_CORRECTIONS = PostgresColumnDefinition("total_autocorrections", PostgresDatatype.INTEGER)
        val TOTAL_SPACE_CORRECTIONS = PostgresColumnDefinition("total_space_corrections", PostgresDatatype.INTEGER)
        val TOTAL_TRANSPOSITION_CORRECTIONS = PostgresColumnDefinition("total_transposition_corrections", PostgresDatatype.INTEGER)
        val TOTAL_INSERT_KEY_CORRECTIONS = PostgresColumnDefinition("insert_key_corrections", PostgresDatatype.INTEGER)
        val TOTAL_RETRO_CORRECTIONS = PostgresColumnDefinition("total_retro_corrections", PostgresDatatype.INTEGER)
        val TOTAL_SKIP_TOUCH_CORRECTIONS = PostgresColumnDefinition("total_skip_touch_corrections", PostgresDatatype.INTEGER)
        val TOTAL_NEAR_KEY_CORRECTIONS = PostgresColumnDefinition("total_near_key_corrections", PostgresDatatype.INTEGER)
        val TOTAL_SUBSTITUTION_CORRECTIONS = PostgresColumnDefinition("total_substitution_corrections", PostgresDatatype.INTEGER)
        val TOTAL_TEST_HIT_CORRECTIONS = PostgresColumnDefinition("total_test_hit_corrections", PostgresDatatype.INTEGER)
        val TOTAL_TYPING_DURATION = PostgresColumnDefinition("total_typing_duration", PostgresDatatype.DOUBLE) // seconds
        val TOTAL_PATH_PAUSES = PostgresColumnDefinition("total_path_pauses", PostgresDatatype.INTEGER)
        val TOTAL_PAUSES = PostgresColumnDefinition("total_pauses", PostgresDatatype.INTEGER)
        val TOTAL_TYPING_EPISODES = PostgresColumnDefinition("total_typing_episodes", PostgresDatatype.INTEGER)
        val SENTIMENT = PostgresColumnDefinition("sentiment", PostgresDatatype.TEXT)
        val SENTIMENT_WORD_COUNT = PostgresColumnDefinition("sentiment_word_count", PostgresDatatype.INTEGER)
        val SENTIMENT_EMOJI_COUNT = PostgresColumnDefinition("sentiment_emoji_count", PostgresDatatype.INTEGER)
        val TYPING_SPEED = PostgresColumnDefinition("typing_speed", PostgresDatatype.DOUBLE)
        val PATH_TYPING_SPEED = PostgresColumnDefinition("path_typing_speed", PostgresDatatype.DOUBLE)

        val columnTypes : Map<String, PostgresDatatype> = redshiftColumns().associate { it.name to it.datatype }

        @JvmStatic
        fun redshiftColumns(): List<PostgresColumnDefinition> {
            return (RedshiftColumns::class.java.fields.asList() + RedshiftColumns::class.java.declaredFields)
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
