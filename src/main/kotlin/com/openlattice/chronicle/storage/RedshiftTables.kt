package com.openlattice.chronicle.storage

import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APPLICATION_LABEL
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_PACKAGE_NAME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.DURATION
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.END_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.INTERACTION_TYPE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.START_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMESTAMP
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMEZONE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.USER
import com.openlattice.postgres.PostgresColumnDefinition
import com.openlattice.postgres.RedshiftTableDefinition

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class RedshiftTables {
    companion object {
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
                        USER,
                        APPLICATION_LABEL
                )

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

        private val USAGE_EVENT_COLS = CHRONICLE_USAGE_EVENTS.columns.joinToString(",") { it.name }
        private val USAGE_EVENT_PARAMS = CHRONICLE_USAGE_EVENTS.columns.joinToString(",") { "?" }

        /**
         * Inserts a row into the usage events table.
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
        val INSERT_USAGE_EVENT_SQL = """
        INSERT INTO $CHRONICLE_USAGE_EVENTS (${USAGE_EVENT_COLS}) VALUES (USAGE_EVENT_PARAMS) ON CONFLICT DO NOTHING
        """.trimIndent()


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

        val INSERT_USAGE_EVENT_COLUMN_INDICES: Map<String, Int> = CHRONICLE_USAGE_EVENTS.columns.mapIndexed { index, pcd -> pcd.name to index }.toMap()
        val INSERT_USAGE_STATS_COLUMN_INDICES: Map<String, Int> = CHRONICLE_USAGE_STATS.columns.mapIndexed { index, pcd -> pcd.name to index }.toMap()
        fun getInsertUsageEventColumnIndex( column: PostgresColumnDefinition ) : Int = INSERT_USAGE_EVENT_COLUMN_INDICES.getValue(column.name)
        fun getInsertUsageStatColumnIndex( column: PostgresColumnDefinition ) : Int = INSERT_USAGE_STATS_COLUMN_INDICES.getValue(column.name)
    }
}


