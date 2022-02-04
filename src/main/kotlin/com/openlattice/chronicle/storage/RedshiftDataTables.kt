package com.openlattice.chronicle.storage

import com.geekbeast.postgres.PostgresColumnDefinition
import com.geekbeast.postgres.RedshiftTableDefinition
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.ACL_KEY
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APPLICATION_LABEL
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_CATEGORY
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_USAGE_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_PACKAGE_NAME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.AUDIT_EVENT_TYPE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.BUNDLE_IDENTIFIER
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.DATA
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.DESCRIPTION
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.DEVICE_MODEL
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.DEVICE_NAME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.DEVICE_SYSTEM_NAME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.DEVICE_VERSION
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.DURATION
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.END_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.INTERACTION_TYPE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.KEYBOARD_IDENTIFIER
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.KEYBOARD_VERSION
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.NOTIFICATION_INTERACTION_TYPE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.PATH_TYPING_SPEED
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.PRINCIPAL_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.RECORDED_DATE_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.SAMPLE_DURATION
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.SAMPLE_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.SECURABLE_PRINCIPAL_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.SENSOR_TYPE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.SENTIMENT
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.SENTIMENT_EMOJI_COUNT
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.SENTIMENT_WORD_COUNT
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.START_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TEXT_INPUT_DEVICE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TEXT_INPUT_DURATION
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMESTAMP
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMEZONE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_ALTERED_WORDS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_AUTO_CORRECTIONS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_CALL_DURATION
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_DELETES
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_DRAGS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_EMOJIS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_INCOMING_CALLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_INCOMING_MESSAGES
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_INSERT_KEY_CORRECTIONS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_NEAR_KEY_CORRECTIONS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_OUTGOING_CALLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_OUTGOING_MESSAGES
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_PATHS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_PATH_LENGTH
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_PATH_PAUSES
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_PATH_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_PAUSES
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_RETRO_CORRECTIONS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_SCREEN_WAKES
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_SKIP_TOUCH_CORRECTIONS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_SPACE_CORRECTIONS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_SUBSTITUTION_CORRECTIONS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_TAPS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_TEST_HIT_CORRECTIONS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_TRANSPOSITION_CORRECTIONS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_TYPING_DURATION
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_TYPING_EPISODES
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_UNIQUE_CONTACTS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_UNLOCK_DURATION
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_WORDS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TYPING_SPEED
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.USERNAME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_CATEGORY_WEB_DURATION

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class RedshiftDataTables {
    companion object {
        const val REDSHIFT_ENVIRONMENT = "redshift"
        internal const val REDSHIFT_DATASOURCE_NAME = "chronicle"

        @JvmField
        val CHRONICLE_USAGE_EVENTS = RedshiftTableDefinition("chronicle_usage_events")
                .sortKey(STUDY_ID)
                .addColumns(
                        ORGANIZATION_ID,
                        STUDY_ID,
                        PARTICIPANT_ID,
                        APP_PACKAGE_NAME,
                        INTERACTION_TYPE,
                        TIMESTAMP,
                        TIMEZONE,
                        USERNAME,
                        APPLICATION_LABEL
                )
                .addDataSourceNames(REDSHIFT_DATASOURCE_NAME)

        @JvmField
        val CHRONICLE_USAGE_STATS = RedshiftTableDefinition("chronicle_usage_stats")
                .sortKey(STUDY_ID)
                .addColumns(
                        ORGANIZATION_ID,
                        STUDY_ID,
                        PARTICIPANT_ID,
                        APP_PACKAGE_NAME,
                        INTERACTION_TYPE,
                        START_TIME,
                        END_TIME,
                        DURATION,
                        TIMESTAMP,
                        TIMEZONE,
                        APPLICATION_LABEL
                )
                .addDataSourceNames(REDSHIFT_DATASOURCE_NAME)


        @JvmField
        val AUDIT = RedshiftTableDefinition("audit")
                .sortKey(ACL_KEY)
                .addColumns(
                        ACL_KEY,
                        SECURABLE_PRINCIPAL_ID,
                        PRINCIPAL_ID,
                        AUDIT_EVENT_TYPE,
                        STUDY_ID,
                        ORGANIZATION_ID,
                        DESCRIPTION,
                        DATA,
                        TIMESTAMP
                )
                .addDataSourceNames(REDSHIFT_DATASOURCE_NAME)

        // applies to all sensor types
        val ALL_SENSOR_COLS = linkedSetOf(
                STUDY_ID,
                PARTICIPANT_ID,
                ID,
                SAMPLE_ID, // uniquely identifies a sample
                SENSOR_TYPE,
                RECORDED_DATE_TIME, //when sample was recorded by framework
                SAMPLE_DURATION,
                START_TIME, // lower date bound for sample record
                END_TIME, // upper date bound for sample record
                DEVICE_VERSION,
                DEVICE_NAME,
                DEVICE_MODEL,
                DEVICE_SYSTEM_NAME
        )

        val DEVICE_USAGE_SENSOR_COLS = linkedSetOf(
                TOTAL_SCREEN_WAKES,
                TOTAL_UNLOCK_DURATION,
                APP_CATEGORY,
                APP_USAGE_TIME,
                TEXT_INPUT_DEVICE,
                TEXT_INPUT_DURATION,
                BUNDLE_IDENTIFIER,
                APP_CATEGORY_WEB_DURATION,
                NOTIFICATION_INTERACTION_TYPE
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
                KEYBOARD_VERSION,
                KEYBOARD_IDENTIFIER,
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
        @JvmField
        val IOS_SENSOR_DATA = RedshiftTableDefinition("sensor_data")
                .sortKey(STUDY_ID)
                .addColumns(
                       *(ALL_SENSOR_COLS + DEVICE_USAGE_SENSOR_COLS + PHONE_USAGE_SENSOR_COLS + MESSAGES_USAGE_SENSOR_COLS +KEYBOARD_METRICS_SENSOR_COLS).toTypedArray()
                ).primaryKey(ID)
                .addDataSourceNames(REDSHIFT_DATASOURCE_NAME)


        private val USAGE_EVENT_COLS = CHRONICLE_USAGE_EVENTS.columns.joinToString(",") { it.name }
        private val USAGE_EVENT_PARAMS = CHRONICLE_USAGE_EVENTS.columns.joinToString(",") { "?" }

        /**
         * Returns the merge clause for matching duplicate rows on insert.
         */
        private fun getMergeClause(srcMergeTableName: String): String {
            return CHRONICLE_USAGE_EVENTS.columns.joinToString(
                    " AND "
            ) { "${CHRONICLE_USAGE_EVENTS.name}.${it.name} = ${srcMergeTableName}.${it.name}" }
        }

        /**
         * Inserts a row into the usage events table.
         * @param srcMergeTableName The name of table that will serve as the source to merge into the
         * CHRONICLE_USAGE_EVENTS table.
         *
         * The bina parameters for this query are in the following order:
         * 1. organization_id (text/uuid)
         * 2. study_id (text/uuid)
         * 3. participant_id (text)
         * 4. app_package_name (text)
         * 5. interaction_type (text)
         * 6. timestamp (timestamptz)
         * 7. timezone (text)
         * 8. user (text)
         * 9. application_label (text)
         */
        fun getInsertIntoMergeUsageEventsTableSql(srcMergeTableName: String, includeOnConflict: Boolean = false): String {
            return if (includeOnConflict) {
                """
                    INSERT INTO $srcMergeTableName (${USAGE_EVENT_COLS}) VALUES (${USAGE_EVENT_PARAMS}) ON CONFLICT DO NOTHING
                    """.trimIndent()
            } else {
                """
                    INSERT INTO $srcMergeTableName (${USAGE_EVENT_COLS}) VALUES (${USAGE_EVENT_PARAMS}) 
                    """.trimIndent()
            }
        }

        fun getDeleteTempTableEntriesSql(srcMergeTableName: String): String {
            return """
            DELETE FROM ${CHRONICLE_USAGE_EVENTS.name} 
                USING $srcMergeTableName 
                WHERE ${getMergeClause(srcMergeTableName)} 
            """.trimIndent()
        }

        fun getAppendTembTableSql(srcMergeTableName: String): String {
            return """
                INSERT INTO ${CHRONICLE_USAGE_EVENTS.name} SELECT * FROM $srcMergeTableName
            """.trimIndent()
        }

        private val USAGE_STAT_COLS = CHRONICLE_USAGE_STATS.columns.joinToString(",") { it.name }
        private val USAGE_STAT_PARAMS = CHRONICLE_USAGE_STATS.columns.joinToString(",") { "?" }

        /**
         * Inserts a row into the usage stats table.
         * 1. organization_id (text/uuid)
         * 2. study_id (text/uuid)
         * 3.
         */
        val INSERT_USAGE_STATS_SQL = """
        INSERT INTO $CHRONICLE_USAGE_EVENTS (${USAGE_EVENT_COLS}) VALUES (USAGE_EVENT_PARAMS) 
        """.trimIndent()

        val INSERT_USAGE_EVENT_COLUMN_INDICES: Map<String, Int> =
                CHRONICLE_USAGE_EVENTS.columns.mapIndexed { index, pcd -> pcd.name to (index + 1) }.toMap() //remeber postgres is 1 based index
        val INSERT_USAGE_STATS_COLUMN_INDICES: Map<String, Int> =
                CHRONICLE_USAGE_STATS.columns.mapIndexed { index, pcd -> pcd.name to (index + 1) }.toMap()

        fun getInsertUsageEventColumnIndex(
                column: PostgresColumnDefinition
        ): Int = INSERT_USAGE_EVENT_COLUMN_INDICES.getValue(column.name)

        fun getInsertUsageStatColumnIndex(
                column: PostgresColumnDefinition
        ): Int = INSERT_USAGE_STATS_COLUMN_INDICES.getValue(column.name)
    }
}


