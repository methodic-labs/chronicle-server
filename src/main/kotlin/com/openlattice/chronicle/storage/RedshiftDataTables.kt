package com.openlattice.chronicle.storage

import com.geekbeast.postgres.PostgresColumnDefinition
import com.geekbeast.postgres.RedshiftTableDefinition
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.ACL_KEY
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APPLICATION_LABEL
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_ENGAGE_30S
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_PACKAGE_NAME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_USAGE_FLAGS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.AUDIT_EVENT_TYPE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.DATA
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.DATE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.DAY
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.DESCRIPTION
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.DEVICE_USAGE_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.DURATION
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.DURATION_SECONDS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.END_DATE_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.END_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.INTERACTION_TYPE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.KEYBOARD_METRICS_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.MESSAGES_USAGE_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.NEW_APP
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.NEW_PERIOD
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.PHONE_USAGE_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.PRINCIPAL_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.PRINCIPAL_TYPE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.RECORD_TYPE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.SECURABLE_PRINCIPAL_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.SHARED_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.START_DATE_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.START_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_SWITCHED_APP
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMESTAMP
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMEZONE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.USERNAME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.WARNING
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.WEEKDAY_MONDAY_FRIDAY
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.WEEKDAY_MONDAY_THURSDAY
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.WEEKDAY_SUNDAY_THURSDAY

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
                STUDY_ID,
                PARTICIPANT_ID,
                APP_PACKAGE_NAME,
                INTERACTION_TYPE,
                TIMESTAMP,
                TIMEZONE,
                USERNAME,
                APPLICATION_LABEL,
            )
            .addDataSourceNames(REDSHIFT_DATASOURCE_NAME)

        @JvmField
        val CHRONICLE_USAGE_STATS = RedshiftTableDefinition("chronicle_usage_stats")
            .sortKey(STUDY_ID)
            .addColumns(
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
                PRINCIPAL_TYPE,
                PRINCIPAL_ID,
                AUDIT_EVENT_TYPE,
                STUDY_ID,
                ORGANIZATION_ID,
                DESCRIPTION,
                DATA,
                TIMESTAMP
            )
            .addDataSourceNames(REDSHIFT_DATASOURCE_NAME)

        @JvmField
        val PREPROCESSED_USAGE_EVENTS = RedshiftTableDefinition("preprocessed_usage_events")
            .sortKey(ACL_KEY)
            .addColumns(
                STUDY_ID,
                PARTICIPANT_ID,
                APPLICATION_LABEL.notNull(),
                APP_PACKAGE_NAME.notNull(),
                START_DATE_TIME,
                END_DATE_TIME,
                TIMEZONE,
                DATE,
                RECORD_TYPE,
                NEW_PERIOD,
                NEW_APP,
                DURATION_SECONDS,
                WARNING,
                DAY,
                WEEKDAY_MONDAY_FRIDAY,
                WEEKDAY_MONDAY_THURSDAY,
                WEEKDAY_SUNDAY_THURSDAY,
                APP_ENGAGE_30S,
                APP_SWITCHED_APP,
                APP_USAGE_FLAGS
            ).addDataSourceNames(REDSHIFT_DATASOURCE_NAME)

        @JvmField
        val IOS_SENSOR_DATA = RedshiftTableDefinition("sensor_data")
            .sortKey(STUDY_ID)
            .addColumns(
                *(SHARED_SENSOR_COLS + DEVICE_USAGE_SENSOR_COLS + PHONE_USAGE_SENSOR_COLS + MESSAGES_USAGE_SENSOR_COLS + KEYBOARD_METRICS_SENSOR_COLS).toTypedArray()
            )
            .addDataSourceNames(REDSHIFT_DATASOURCE_NAME)

        private val INSERT_SENSOR_DATA_COL_INDICES = IOS_SENSOR_DATA.columns.mapIndexed { index, col -> col.name to index + 1 }.toMap()

        fun getInsertSensorDataColumnIndex(col: PostgresColumnDefinition): Int {
            return INSERT_SENSOR_DATA_COL_INDICES.getValue(col.name)
        }

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


