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
        @JvmField val APP_PACKAGE_NAME = PostgresColumnDefinition("app_package_name", PostgresDatatype.TEXT)

        @JvmField val ID = PostgresColumnDefinition("id", PostgresDatatype.TEXT_UUID).notNull()
        //IMPORTANCE in client code base
        @JvmField val INTERACTION_TYPE = PostgresColumnDefinition("interaction_type", PostgresDatatype.TEXT)
        @JvmField val TIMESTAMP = PostgresColumnDefinition("event_timestamp", PostgresDatatype.TIMESTAMPTZ)
        @JvmField val TIMEZONE = PostgresColumnDefinition("timezone", PostgresDatatype.TEXT)
        @JvmField val USERNAME = PostgresColumnDefinition("username", PostgresDatatype.TEXT)
        @JvmField val APPLICATION_LABEL = PostgresColumnDefinition("application_label", PostgresDatatype.TEXT)
        @JvmField val START_TIME = PostgresColumnDefinition("start_time", PostgresDatatype.TIMESTAMPTZ )
        @JvmField val END_TIME = PostgresColumnDefinition("end_time", PostgresDatatype.TIMESTAMPTZ )
        //Convert all duration to milliseconds for redshift compatibility
        @JvmField val DURATION = PostgresColumnDefinition("duration", PostgresDatatype.BIGINT )

        val FQNS_TO_USAGE_EVENT_COLUMNS: Map<FullQualifiedName, PostgresColumnDefinition> = mapOf(
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

        @JvmField val ORGANIZATION_ID = PostgresColumnDefinition("organization_id", PostgresDatatype.TEXT_UUID).notNull()
        @JvmField val STUDY_ID = PostgresColumnDefinition("study_id", PostgresDatatype.TEXT_UUID).notNull()
        @JvmField val PARTICIPANT_ID = PostgresColumnDefinition("participant_id", PostgresDatatype.TEXT).notNull()


        @JvmField val COMPLETED = PostgresColumnDefinition(EdmConstants.COMPLETED_DATE_TIME_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        @JvmField val DATE_LOGGED = PostgresColumnDefinition(EdmConstants.DATE_LOGGED_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        @JvmField val DATE = PostgresColumnDefinition(EdmConstants.DATE_TIME_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        @JvmField val END_DATE_TIME = PostgresColumnDefinition(EdmConstants.END_DATE_TIME_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        @JvmField  val MODEL = PostgresColumnDefinition(EdmConstants.MODEL_FQN.name, PostgresDatatype.TEXT)
        @JvmField val OL_ID = PostgresColumnDefinition(EdmConstants.OL_ID_FQN.name, PostgresDatatype.TEXT)
        @JvmField val PERSON_ID = PostgresColumnDefinition(EdmConstants.PERSON_ID_FQN.name, PostgresDatatype.TEXT)

        @JvmField val RECORDED_DATE_TIME = PostgresColumnDefinition(EdmConstants.RECORDED_DATE_TIME_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        @JvmField val START_DATE_TIME = PostgresColumnDefinition(EdmConstants.START_DATE_TIME_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        @JvmField val STATUS = PostgresColumnDefinition(EdmConstants.STATUS_FQN.name, PostgresDatatype.TEXT)

        @JvmField val STRING_ID = PostgresColumnDefinition(EdmConstants.STRING_ID_FQN.name, PostgresDatatype.TEXT)
        @JvmField val TITLE = PostgresColumnDefinition(EdmConstants.TITLE_FQN.name, PostgresDatatype.TEXT)
        @JvmField val VALUES = PostgresColumnDefinition(EdmConstants.VALUES_FQN.name, PostgresDatatype.TEXT)
        @JvmField val VERSION = PostgresColumnDefinition(EdmConstants.VERSION_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        @JvmField val NEW_APP = PostgresColumnDefinition(EdmConstants.NEW_APP_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        @JvmField val GENERAL_END_TIME = PostgresColumnDefinition(EdmConstants.GENERAL_END_TIME_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        @JvmField val NEW_PERIOD = PostgresColumnDefinition(EdmConstants.NEW_PERIOD_FQN.name, PostgresDatatype.TIMESTAMPTZ)
        @JvmField val WARNING = PostgresColumnDefinition(EdmConstants.WARNING_FQN.name, PostgresDatatype.TIMESTAMPTZ)

        //AUDIT
        @JvmField val SECURABLE_PRINCIPAL_ID = PostgresColumnDefinition("id", PostgresDatatype.TEXT_UUID).notNull()
        @JvmField val PRINCIPAL_ID = PostgresColumnDefinition("principal_id", PostgresDatatype.TEXT_256).notNull()
        @JvmField val ACL_KEY = PostgresColumnDefinition("acl_key", PostgresDatatype.TEXT_256).notNull()
        @JvmField val AUDIT_EVENT_TYPE = PostgresColumnDefinition("audit_event_type", PostgresDatatype.TEXT_256).notNull()
        @JvmField val DESCRIPTION = PostgresColumnDefinition("description", PostgresDatatype.TEXT_256).notNull()
        @JvmField val DATA = PostgresColumnDefinition("data", PostgresDatatype.VARCHAR_MAX).notNull()

        // SENSOR_DATA
        // these columns apply to all sensor types
        @JvmField val SAMPLE_ID = PostgresColumnDefinition("sample_id", PostgresDatatype.TEXT_UUID).notNull()
        @JvmField val SENSOR_TYPE = PostgresColumnDefinition("sensor_type", PostgresDatatype.TEXT).notNull()
        @JvmField val SAMPLE_DURATION = PostgresColumnDefinition("sample_duration", PostgresDatatype.DOUBLE).notNull() // seconds
        @JvmField val DEVICE_VERSION = PostgresColumnDefinition("device_version", PostgresDatatype.TEXT).notNull()
        @JvmField val DEVICE_NAME = PostgresColumnDefinition("device_name", PostgresDatatype.TEXT).notNull()
        @JvmField val DEVICE_MODEL = PostgresColumnDefinition("device_model", PostgresDatatype.TEXT).notNull()
        @JvmField  val DEVICE_SYSTEM_NAME = PostgresColumnDefinition("device_system_name", PostgresDatatype.TEXT).notNull()

        // columns specific to deviceUsage sensor
        @JvmField val TOTAL_SCREEN_WAKES = PostgresColumnDefinition("total_screen_wakes", PostgresDatatype.INTEGER)
        @JvmField val TOTAL_UNLOCK_DURATION = PostgresColumnDefinition("total_unlock_duration", PostgresDatatype.DOUBLE) // seconds
        @JvmField val TOTAL_UNLOCKS = PostgresColumnDefinition("total_unlocks", PostgresDatatype.INTEGER)
        @JvmField val APP_CATEGORY = PostgresColumnDefinition("app_category", PostgresDatatype.TEXT)
        @JvmField val APP_USAGE_TIME = PostgresColumnDefinition("app_usage_time", PostgresDatatype.DOUBLE)
        @JvmField val TEXT_INPUT_SOURCE = PostgresColumnDefinition("text_input_source", PostgresDatatype.TEXT)
        @JvmField val TEXT_INPUT_DURATION = PostgresColumnDefinition("text_input_duration", PostgresDatatype.DOUBLE)
        @JvmField val BUNDLE_IDENTIFIER = PostgresColumnDefinition("bundle_identifier", PostgresDatatype.TEXT)
        @JvmField val APP_CATEGORY_WEB_DURATION = PostgresColumnDefinition("app_category_web_duration", PostgresDatatype.DOUBLE)

        // columns specific to phoneUsage sensor
        @JvmField val TOTAL_INCOMING_CALLS = PostgresColumnDefinition("total_incoming_calls", PostgresDatatype.INTEGER)
        @JvmField val TOTAL_OUTGOING_CALLS = PostgresColumnDefinition("total_outgoing_calls", PostgresDatatype.INTEGER)
        @JvmField val TOTAL_CALL_DURATION = PostgresColumnDefinition("total_call_duration", PostgresDatatype.DOUBLE)
        @JvmField val TOTAL_UNIQUE_CONTACTS = PostgresColumnDefinition("total_unique_contacts", PostgresDatatype.INTEGER)

        // messagesUsage sensor columns
        @JvmField val TOTAL_INCOMING_MESSAGES = PostgresColumnDefinition("total_incoming_messages", PostgresDatatype.INTEGER)
        @JvmField val TOTAL_OUTGOING_MESSAGES = PostgresColumnDefinition("total_outgoing_messages", PostgresDatatype.INTEGER)

        // keyboard metrics sensor columns
        @JvmField val KEYBOARD_VERSION = PostgresColumnDefinition("keyboard_version", PostgresDatatype.TEXT)
        @JvmField val KEYBOARD_IDENTIFIER = PostgresColumnDefinition("keyboard_identifier", PostgresDatatype.TEXT)
        @JvmField val TOTAL_WORDS = PostgresColumnDefinition("total_words", PostgresDatatype.INTEGER)
        @JvmField val TOTAL_ALTERED_WORDS = PostgresColumnDefinition("total_altered_words", PostgresDatatype.INTEGER)
        @JvmField val TOTAL_TAPS = PostgresColumnDefinition("total_taps", PostgresDatatype.INTEGER)
        @JvmField val TOTAL_DRAGS = PostgresColumnDefinition("total_drags", PostgresDatatype.INTEGER)
        @JvmField val TOTAL_DELETES = PostgresColumnDefinition("total_deletes", PostgresDatatype.INTEGER)
        @JvmField val TOTAL_EMOJIS = PostgresColumnDefinition("total_emojis", PostgresDatatype.INTEGER)
        @JvmField val TOTAL_PATHS = PostgresColumnDefinition("total_paths", PostgresDatatype.INTEGER)
        @JvmField val TOTAL_PATH_TIME = PostgresColumnDefinition("total_path_time", PostgresDatatype.DOUBLE)
        @JvmField val TOTAL_PATH_LENGTH = PostgresColumnDefinition("total_path_length", PostgresDatatype.DOUBLE)
        @JvmField val TOTAL_AUTO_CORRECTIONS = PostgresColumnDefinition("total_autocorrections", PostgresDatatype.INTEGER)
        @JvmField val TOTAL_SPACE_CORRECTIONS = PostgresColumnDefinition("total_space_corrections", PostgresDatatype.INTEGER)
        @JvmField  val TOTAL_TRANSPOSITION_CORRECTIONS = PostgresColumnDefinition("total_transposition_corrections", PostgresDatatype.INTEGER)
        @JvmField val TOTAL_INSERT_KEY_CORRECTIONS = PostgresColumnDefinition("insert_key_corrections", PostgresDatatype.INTEGER)
        @JvmField val TOTAL_RETRO_CORRECTIONS = PostgresColumnDefinition("total_retro_corrections", PostgresDatatype.INTEGER)
        @JvmField val TOTAL_SKIP_TOUCH_CORRECTIONS = PostgresColumnDefinition("total_skip_touch_corrections", PostgresDatatype.INTEGER)
        @JvmField val TOTAL_NEAR_KEY_CORRECTIONS = PostgresColumnDefinition("total_near_key_corrections", PostgresDatatype.INTEGER)
        @JvmField val TOTAL_SUBSTITUTION_CORRECTIONS = PostgresColumnDefinition("total_substitution_corrections", PostgresDatatype.INTEGER)
        @JvmField val TOTAL_TEST_HIT_CORRECTIONS = PostgresColumnDefinition("total_test_hit_corrections", PostgresDatatype.INTEGER)
        @JvmField val TOTAL_TYPING_DURATION = PostgresColumnDefinition("total_typing_duration", PostgresDatatype.DOUBLE) // seconds
        @JvmField val TOTAL_PATH_PAUSES = PostgresColumnDefinition("total_path_pauses", PostgresDatatype.INTEGER)
        @JvmField val TOTAL_PAUSES = PostgresColumnDefinition("total_pauses", PostgresDatatype.INTEGER)
        @JvmField val TOTAL_TYPING_EPISODES = PostgresColumnDefinition("total_typing_episodes", PostgresDatatype.INTEGER)
        @JvmField val SENTIMENT = PostgresColumnDefinition("sentiment", PostgresDatatype.TEXT)
        @JvmField val SENTIMENT_WORD_COUNT = PostgresColumnDefinition("sentiment_word_count", PostgresDatatype.INTEGER)
        @JvmField val SENTIMENT_EMOJI_COUNT = PostgresColumnDefinition("sentiment_emoji_count", PostgresDatatype.INTEGER)
        @JvmField val TYPING_SPEED = PostgresColumnDefinition("typing_speed", PostgresDatatype.DOUBLE)
        @JvmField val PATH_TYPING_SPEED = PostgresColumnDefinition("path_typing_speed", PostgresDatatype.DOUBLE)

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

        // applies to all sensor types
        val SHARED_SENSOR_COLS = linkedSetOf(
                STUDY_ID,
                PARTICIPANT_ID,
                SAMPLE_ID, // uniquely identifies a sample
                SENSOR_TYPE,
                SAMPLE_DURATION,
                RECORDED_DATE_TIME, //when sample was recorded by framework
                START_DATE_TIME, // lower date bound for sample record
                END_DATE_TIME, // upper date bound for sample record
                TIMEZONE,
                DEVICE_VERSION,
                DEVICE_NAME,
                DEVICE_MODEL,
                DEVICE_SYSTEM_NAME
        )

        val DEVICE_USAGE_SENSOR_COLS = linkedSetOf(
                TOTAL_SCREEN_WAKES,
                TOTAL_UNLOCK_DURATION,
                TOTAL_UNLOCKS,
                APP_CATEGORY,
                APP_USAGE_TIME,
                TEXT_INPUT_SOURCE,
                TEXT_INPUT_DURATION,
                BUNDLE_IDENTIFIER,
                APP_CATEGORY_WEB_DURATION
        )

        val PHONE_USAGE_SENSOR_COLS = linkedSetOf(
                TOTAL_INCOMING_CALLS,
                TOTAL_OUTGOING_CALLS,
                TOTAL_CALL_DURATION,
                TOTAL_UNIQUE_CONTACTS
        )

        val MESSAGES_USAGE_SENSOR_COLS = linkedSetOf(
                TOTAL_INCOMING_MESSAGES,
                TOTAL_OUTGOING_MESSAGES,
                TOTAL_UNIQUE_CONTACTS
        )

        val KEYBOARD_METRICS_SENSOR_COLS = linkedSetOf(
                TOTAL_WORDS,
                TOTAL_ALTERED_WORDS,
                TOTAL_TAPS,
                TOTAL_DRAGS,
                TOTAL_DELETES,
                TOTAL_EMOJIS,
                TOTAL_PATHS,
                TOTAL_PATH_TIME, //time to complete paths in seconds
                TOTAL_PATH_LENGTH, //length of completed paths in cm
                TOTAL_AUTO_CORRECTIONS,
                TOTAL_SPACE_CORRECTIONS,
                TOTAL_TRANSPOSITION_CORRECTIONS,
                TOTAL_INSERT_KEY_CORRECTIONS,
                TOTAL_RETRO_CORRECTIONS,
                TOTAL_SKIP_TOUCH_CORRECTIONS,
                TOTAL_NEAR_KEY_CORRECTIONS,
                TOTAL_SUBSTITUTION_CORRECTIONS,
                TOTAL_TEST_HIT_CORRECTIONS,
                TOTAL_TYPING_DURATION, // seconds
                TOTAL_PATH_PAUSES, //number of pauses while drawing path for a word
                TOTAL_PAUSES,
                TOTAL_TYPING_EPISODES, //number of continuous typing episodes
                SENTIMENT,
                SENTIMENT_WORD_COUNT,
                SENTIMENT_EMOJI_COUNT,
                TYPING_SPEED, // characters per second
                PATH_TYPING_SPEED //QuickType words per minute
        )
    }
}
