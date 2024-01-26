package com.openlattice.chronicle.storage

import com.geekbeast.postgres.PostgresColumnDefinition
import com.geekbeast.postgres.PostgresDatatype
import com.geekbeast.postgres.PostgresTableDefinition
import com.geekbeast.postgres.RedshiftTableDefinition
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.MAX_BIND_PARAMETERS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.ACL_KEY
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APPLICATION_LABEL
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_DATETIME_END
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_DATETIME_START
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_DURATION_SECONDS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_ENGAGE_30S
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_FULL_NAME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_PACKAGE_NAME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_RECORD_TYPE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_SWITCHED_APP
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_TIMEZONE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_TITLE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_USAGE_FLAGS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.AUDIT_EVENT_TYPE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.DATA
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.DAY
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.DESCRIPTION
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.DEVICE_USAGE_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.DURATION
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.END_DATE_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.END_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.EVENT_TYPE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.EXACT_RECORDED_DATE_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.INTERACTION_TYPE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.IOS_UTILITY_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.KEYBOARD_METRICS_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.MESSAGES_USAGE_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.PHONE_USAGE_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.PRINCIPAL_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.PRINCIPAL_TYPE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.RECORDED_DATE_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.RUN_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.SAMPLE_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.SECURABLE_PRINCIPAL_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.SHARED_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.START_DATE_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.START_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMESTAMP
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMEZONE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.UPLOADED_AT
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.USERNAME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.WEEKDAY_MONDAY_FRIDAY
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.WEEKDAY_MONDAY_THURSDAY
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.WEEKDAY_SUNDAY_THURSDAY
import java.security.InvalidParameterException
import java.time.LocalDate

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
                EVENT_TYPE,
                TIMESTAMP,
                TIMEZONE,
                USERNAME,
                APPLICATION_LABEL,
                UPLOADED_AT
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
            .sortKey(STUDY_ID)
            .addColumns(
                RUN_ID,
                STUDY_ID,
                PARTICIPANT_ID,
                APP_RECORD_TYPE,
                APP_TITLE,
                APP_FULL_NAME,
                APP_DATETIME_START,
                APP_DATETIME_END,
                APP_TIMEZONE,
                APP_DURATION_SECONDS,
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
                *(SHARED_SENSOR_COLS + DEVICE_USAGE_SENSOR_COLS + PHONE_USAGE_SENSOR_COLS + MESSAGES_USAGE_SENSOR_COLS + KEYBOARD_METRICS_SENSOR_COLS + IOS_UTILITY_COLS).toTypedArray()
            )
            .addDataSourceNames(REDSHIFT_DATASOURCE_NAME)

        private val INSERT_SENSOR_DATA_COL_INDICES =
            IOS_SENSOR_DATA.columns.mapIndexed { index, col -> col.name to index + 1 }.toMap()

        fun getInsertSensorDataColumnIndex(col: PostgresColumnDefinition): Int {
            return INSERT_SENSOR_DATA_COL_INDICES.getValue(col.name)
        }

        private val USAGE_EVENT_COLS = CHRONICLE_USAGE_EVENTS.columns.joinToString(",") { it.name }
        private val USAGE_EVENT_PARAMS = CHRONICLE_USAGE_EVENTS.columns.joinToString(",") { "?" }


        /**
         * Returns the merge clause for matching duplicate rows on insert.
         */
        private fun getMergeClause(
            srcMergeTableName: String,
            table: PostgresTableDefinition = CHRONICLE_USAGE_EVENTS,
            columnsToExclude: Set<PostgresColumnDefinition> = setOf()
        ): String {
            //These are the columns
            return (table.columns - columnsToExclude).joinToString(
                " AND "
            ) {
                val defaultValue = when (it.datatype) {
                    PostgresDatatype.BOOLEAN -> "false"
                    PostgresDatatype.TEXT_UUID, PostgresDatatype.TEXT, PostgresDatatype.TEXT_128, PostgresDatatype.TEXT_256, PostgresDatatype.TEXT_512 -> "''"
                    PostgresDatatype.BIGINT, PostgresDatatype.INTEGER, PostgresDatatype.NUMERIC, PostgresDatatype.DECIMAL, PostgresDatatype.DOUBLE, PostgresDatatype.REAL, PostgresDatatype.SMALLINT, PostgresDatatype.SERIAL, PostgresDatatype.BIGSERIAL -> "0"
                    PostgresDatatype.DATE -> LocalDate.MIN.toString()
                    PostgresDatatype.TIMESTAMP, PostgresDatatype.TIMESTAMPTZ -> "to_timestamp(0, 'utc')"
                    PostgresDatatype.SMALLINT_ARRAY,PostgresDatatype.UUID_ARRAY,PostgresDatatype.INTEGER_ARRAY, PostgresDatatype.TIMETZ_ARRAY, PostgresDatatype.TIMESTAMPTZ_ARRAY, PostgresDatatype.TIME_ARRAY, PostgresDatatype.BYTEA_ARRAY, PostgresDatatype.DATE_ARRAY, PostgresDatatype.DOUBLE_ARRAY, PostgresDatatype.BIGINT_ARRAY, PostgresDatatype.BOOLEAN_ARRAY -> "ARRAY()"
                    else -> InvalidParameterException("Unsupported data type column encountered.")
                }
                "COALESCE(${table.name}.${it.name},$defaultValue) = COALESCE(${srcMergeTableName}.${it.name},$defaultValue)"
            }
        }

        /**
         * Returns the merge clause for matching duplicate rows on insert for ios data. Sample id is worthless
         * as the same data can come back from multiple sample ids due to multiple invocations of sensorkit framework.
         */
        private fun getIosMergeClause(srcMergeTableName: String): String {
            //Sample ID is worthless we need to delete eventually
            return (IOS_SENSOR_DATA.columns - SAMPLE_ID).joinToString(
                " AND "
            ) { "${IOS_SENSOR_DATA.name}.${it.name} = ${srcMergeTableName}.${it.name}" }
        }

        /**
         * Inserts a row into the usage events table.
         * @param tableName The name of table that will serve as the source to merge into the
         * CHRONICLE_USAGE_EVENTS table.
         *
         * The bina parameters for this query are in the following order:
         * 1. organization_id (text/uuid)
         * 2. study_id (text/uuid)
         * 3. participant_id (text)
         * 4. app_package_name (text)
         * 5. interaction_type (text)
         * 6. event_type (int)
         * 7. timestamp (timestamptz)
         * 8. timezone (text)
         * 9. user (text)
         * 10. application_label (text)
         */
        fun getInsertIntoUsageEventsTableSql(tableName: String, includeOnConflict: Boolean = false): String {
            return if (includeOnConflict) {
                """
                    INSERT INTO $tableName (${USAGE_EVENT_COLS}) VALUES (${USAGE_EVENT_PARAMS}) ON CONFLICT DO NOTHING
                    """.trimIndent()
            } else {
                """
                    INSERT INTO $tableName (${USAGE_EVENT_COLS}) VALUES (${USAGE_EVENT_PARAMS}) 
                    """.trimIndent()
            }
        }

        /**
         * Builds a multi-line prepared statement for inserting batches of data into redshift.
         * @param numLines The number of lines containing usage events to insert
         * @param includeOnConflict Whether or not it should include the on conflict statement
         */
        fun buildMultilineInsertUsageEvents(numLines: Int, includeOnConflict: Boolean): String {
            check((CHRONICLE_USAGE_EVENTS.columns.size * numLines) < MAX_BIND_PARAMETERS) {
                "Maximum number of postgres bind parameters would be exceeded with this amount of lines"
            }
            val columns = CHRONICLE_USAGE_EVENTS.columns.joinToString(",") { it.name }
            val header = "INSERT INTO ${CHRONICLE_USAGE_EVENTS.name} ($columns) VALUES"
            val params = CHRONICLE_USAGE_EVENTS.columns.joinToString(",") { "?" }
            val line = "($params)"
            val lines = (1..numLines).joinToString(",\n") { line }

            check((header.length + (line.length * numLines)) < 16777216) {
                "SQL exceeds maximum length allowed for redshift."
            }

            return if (includeOnConflict) {
                "$header\n$lines ON CONFLICT DO NOTHING"
            } else {
                "$header\n$lines"
            }
        }

        /**
         * Builds a multi-line prepared statement for inserting batches of data into redshift.
         * @param numLines The number of lines containing usage events to insert
         * @param includeOnConflict Whether or not it should include the on conflict statement
         */
        fun buildMultilineInsertSensorEvents(numLines: Int, includeOnConflict: Boolean): String {
            check((IOS_SENSOR_DATA.columns.size * numLines) < MAX_BIND_PARAMETERS) {
                "Maximum number of postgres bind parameters would be exceeded with this amount of lines"
            }
            val columns = IOS_SENSOR_DATA.columns.joinToString(",") { it.name }
            val header = "INSERT INTO ${IOS_SENSOR_DATA.name} ($columns) VALUES"
            val params = IOS_SENSOR_DATA.columns.joinToString(",") { "?" }
            val line = "($params)"
            val lines = (1..numLines).joinToString(",\n") { line }

            check((header.length + (line.length * numLines)) < 16777216) {
                "SQL exceeds maximum length allowed for redshift."
            }

            return if (includeOnConflict) {
                "$header\n$lines ON CONFLICT DO NOTHING"
            } else {
                "$header\n$lines"
            }
        }

        /**
         * Builds a multi-line prepared statement for inserting batches of audit events into redshift.
         * @param numLines The number of lines containing usage events to insert
         * @param includeOnConflict Whether or not it should include the on conflict statement
         */
        fun buildMultilineInsertAuditEvents(numLines: Int, includeOnConflict: Boolean): String {
            check((AUDIT.columns.size * numLines) < MAX_BIND_PARAMETERS) {
                "Maximum number of postgres bind parameters would be exceeded with this amount of lines"
            }

            val columns = AUDIT.columns.joinToString(",") { it.name }
            val header = "INSERT INTO ${AUDIT.name} ($columns) VALUES"
            val params = AUDIT.columns.joinToString(",") { "?" }
            val line = "($params)"
            val lines = (1..numLines).joinToString(",\n") { line }

            check((header.length + (line.length * numLines)) < 16777216) {
                "SQL exceeds maximum length allowed for redshift."
            }

            return if (includeOnConflict) {
                "$header\n$lines ON CONFLICT DO NOTHING"
            } else {
                "$header\n$lines"
            }
        }


        /**
         * Generates sql for creating a temp table of duplicates that may have been inserted into redshift.
         *
         * Default chronicle usage events table
         * 1. study id
         * 2. participant id
         * 3. event_timestamp lowerbound
         * 4. event_timestamp upperbound.
         * @param tempTableName The
         */
        fun createTempTableOfDuplicates(
            tempTableName: String,
            likeTable: PostgresTableDefinition = CHRONICLE_USAGE_EVENTS
        ): String {
            return """
                CREATE TEMPORARY TABLE $tempTableName (LIKE ${likeTable.name}) 
            """.trimIndent()
        }

        fun buildTempTableOfDuplicates(tempTableName: String): String {
            val groupByCols = (CHRONICLE_USAGE_EVENTS.columns - UPLOADED_AT).joinToString(",") { it.name }
            return """
                INSERT INTO $tempTableName ($groupByCols,${UPLOADED_AT.name}) SELECT $groupByCols, min(${UPLOADED_AT.name}) as ${UPLOADED_AT.name} FROM ${CHRONICLE_USAGE_EVENTS.name}
                                        WHERE ${STUDY_ID.name} = ANY(?) AND ${PARTICIPANT_ID.name} = ANY(?) AND
                                            ${TIMESTAMP.name} >= ? AND ${TIMESTAMP.name} <= ? 
                                        GROUP BY $groupByCols
                                        HAVING count(${UPLOADED_AT.name}) > 1
            """.trimIndent()
        }

        fun buildTempTableOfDuplicatesForIos(tempTableName: String): String {
            val excluded = setOf(
                SAMPLE_ID,
                START_DATE_TIME,
                END_DATE_TIME,
                EXACT_RECORDED_DATE_TIME
            )
            //TODO: We don't have to run this on every insert, we could run it every 15 minutes to be more efficient,
            //with the trade off that users would see duplicates in downloads until it ran.
            val groupByCols = (IOS_SENSOR_DATA.columns - excluded).joinToString(",") { it.name }

            val excludedCols = excluded.joinToString(",") { it.name }
            return """
                INSERT INTO $tempTableName ($groupByCols,$excludedCols) 
                    SELECT $groupByCols, min(${SAMPLE_ID.name}) as ${SAMPLE_ID.name},min(${START_DATE_TIME.name}) as ${START_DATE_TIME.name},max(${END_DATE_TIME.name}) as ${END_DATE_TIME.name},
                    max(${EXACT_RECORDED_DATE_TIME.name}) as ${EXACT_RECORDED_DATE_TIME.name}
                        FROM ${IOS_SENSOR_DATA.name}
                        WHERE ${STUDY_ID.name} = ANY(?) AND ${PARTICIPANT_ID.name} = ANY(?) 
                            AND ${RECORDED_DATE_TIME.name} >= ? AND ${RECORDED_DATE_TIME.name} <= ? 
                        GROUP BY $groupByCols
                        HAVING count(${SAMPLE_ID.name}) > 1
            """.trimIndent()
        }

        fun getDeleteIosSensorDataFromTempTable(tempTableName: String): String {
            return """
            DELETE FROM ${IOS_SENSOR_DATA.name} 
                USING $tempTableName 
                WHERE ${
                getMergeClause(
                    tempTableName,
                    table = IOS_SENSOR_DATA,
                    setOf(SAMPLE_ID, START_DATE_TIME, END_DATE_TIME, EXACT_RECORDED_DATE_TIME)
                )
            } 
            """.trimIndent()
        }

        fun getDeleteUsageEventsFromTempTable(tempTableName: String): String {
            return """
            DELETE FROM ${CHRONICLE_USAGE_EVENTS.name} 
                USING $tempTableName 
                WHERE ${getMergeClause(tempTableName)} 
            """.trimIndent()
        }

        fun getAppendTempTableSql(srcMergeTableName: String): String {

            return """
                INSERT INTO ${CHRONICLE_USAGE_EVENTS.name} ($USAGE_EVENT_COLS) SELECT $USAGE_EVENT_COLS FROM $srcMergeTableName
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
            CHRONICLE_USAGE_EVENTS.columns.mapIndexed { index, pcd -> pcd.name to (index + 1) }
                .toMap() //remeber postgres is 1 based index
        val INSERT_USAGE_STATS_COLUMN_INDICES: Map<String, Int> =
            CHRONICLE_USAGE_STATS.columns.mapIndexed { index, pcd -> pcd.name to (index + 1) }.toMap()

        const val UNIQUE_DATES = "unique_dates"
        val participantStatsIosSql = """
                SELECT ${STUDY_ID.name}, ${PARTICIPANT_ID.name}, listagg(distinct TRUNC(${RECORDED_DATE_TIME.name} at time zone ${TIMEZONE.name}), ',') as $UNIQUE_DATES
                FROM ${IOS_SENSOR_DATA.name} 
                WHERE ${STUDY_ID.name} = ?
                GROUP BY ${STUDY_ID.name}, ${PARTICIPANT_ID.name}
            """.trimIndent()

        val participantStatsAndroidSql = """
                SELECT ${STUDY_ID.name}, ${PARTICIPANT_ID.name}, listagg(distinct TRUNC(${TIMESTAMP.name} at time zone ${TIMEZONE.name}), ',') as $UNIQUE_DATES
                FROM ${CHRONICLE_USAGE_EVENTS.name}
                WHERE ${STUDY_ID.name} = ? AND timezone != ''
                GROUP BY ${STUDY_ID.name}, ${PARTICIPANT_ID.name}
            """.trimIndent()

        fun getInsertUsageEventColumnIndex(
            column: PostgresColumnDefinition,
        ): Int = INSERT_USAGE_EVENT_COLUMN_INDICES.getValue(column.name)

        fun getInsertUsageEventColumnIndex(
            columnName: String
        ): Int = INSERT_USAGE_EVENT_COLUMN_INDICES.getValue(columnName)

        fun getInsertUsageStatColumnIndex(
            column: PostgresColumnDefinition,
        ): Int = INSERT_USAGE_STATS_COLUMN_INDICES.getValue(column.name)


    }
}


