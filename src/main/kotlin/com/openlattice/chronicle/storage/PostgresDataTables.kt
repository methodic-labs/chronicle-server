package com.openlattice.chronicle.storage

import com.geekbeast.postgres.PostgresColumnDefinition
import com.geekbeast.postgres.PostgresColumnsIndexDefinition
import com.geekbeast.postgres.PostgresDatatype
import com.geekbeast.postgres.PostgresTableDefinition
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.MAX_BIND_PARAMETERS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_DATETIME_START
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.END_DATE_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.EXACT_RECORDED_DATE_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.RECORDED_DATE_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.SAMPLE_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.START_DATE_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMESTAMP
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.UPLOADED_AT

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresDataTables {
    companion object {
        const val POSTGRES_DATA_ENVIRONMENT = "postgres_data"

        /**
         * Two salted hash columns using Postgres built-in hashtextextended().
         * Together they form a 128-bit dedup key (seed 0 and seed 1).
         * The UNIQUE index is on (dedup_hash_0, dedup_hash_1).
         */
        @JvmField
        val DEDUP_HASH_0 = PostgresColumnDefinition("dedup_hash_0", PostgresDatatype.BIGINT).notNull()

        @JvmField
        val DEDUP_HASH_1 = PostgresColumnDefinition("dedup_hash_1", PostgresDatatype.BIGINT).notNull()

        private const val DEDUP_HASH_COLUMNS_SQL = "dedup_hash_0, dedup_hash_1"

        /**
         * Upgrades Redshift-compat varchar(36) uuid columns to native Postgres uuid (16 bytes).
         * TEXT_UUID is a kludge that exists only because Redshift lacks a uuid type; on Postgres
         * the native uuid is smaller on disk, faster to compare, and indexes more densely.
         * All TEXT_UUID columns in the mirrored tables are NOT NULL, so the replacement
         * preserves that constraint.
         */
        private fun nativizeUuid(col: PostgresColumnDefinition): PostgresColumnDefinition =
            if (col.datatype == PostgresDatatype.TEXT_UUID)
                PostgresColumnDefinition(col.name, PostgresDatatype.UUID).notNull()
            else col

        /**
         * Nativized-type copies of the Redshift column lists. These are the source of truth
         * for both the Postgres table definitions and the insert/upsert SQL builders — using
         * them everywhere keeps bind-param casts (`?::uuid`) in sync with the actual column
         * types. Do not read column types off `RedshiftDataTables.*.columns` for Postgres SQL;
         * those still carry `varchar(36)` for Redshift compatibility.
         */
        private val CHRONICLE_USAGE_EVENTS_MIRRORED_COLUMNS: List<PostgresColumnDefinition> =
            RedshiftDataTables.CHRONICLE_USAGE_EVENTS.columns.map(::nativizeUuid)

        private val IOS_SENSOR_DATA_MIRRORED_COLUMNS: List<PostgresColumnDefinition> =
            RedshiftDataTables.IOS_SENSOR_DATA.columns.map(::nativizeUuid)

        @JvmField
        val CHRONICLE_USAGE_EVENTS = PostgresTableDefinition(RedshiftDataTables.CHRONICLE_USAGE_EVENTS.name)
            .addColumns(*CHRONICLE_USAGE_EVENTS_MIRRORED_COLUMNS.toTypedArray())
            .addColumns(DEDUP_HASH_0, DEDUP_HASH_1)
            .setUnique(DEDUP_HASH_0, DEDUP_HASH_1)
            .addDataSourceNames(RedshiftDataTables.REDSHIFT_DATASOURCE_NAME)

        @JvmField
        val CHRONICLE_USAGE_STATS = PostgresTableDefinition(RedshiftDataTables.CHRONICLE_USAGE_STATS.name)
            .addColumns(*RedshiftDataTables.CHRONICLE_USAGE_STATS.columns.map(::nativizeUuid).toTypedArray())
            .addColumns(DEDUP_HASH_0, DEDUP_HASH_1)
            .setUnique(DEDUP_HASH_0, DEDUP_HASH_1)
            .addDataSourceNames(RedshiftDataTables.REDSHIFT_DATASOURCE_NAME)

        @JvmField
        val AUDIT = PostgresTableDefinition(RedshiftDataTables.AUDIT.name)
            .addColumns(
                *RedshiftDataTables.AUDIT.columns.map { col ->
                    when (col.name) {
                        // acl_key is a serialized UUID path on Redshift (varchar(256) of
                        // hyphen-stripped 32-char hex chunks concatenated). On Postgres we
                        // store it natively as uuid[] — smaller, faster, queryable with
                        // containment operators.
                        "acl_key" -> PostgresColumnDefinition("acl_key", PostgresDatatype.UUID_ARRAY).notNull()
                        else -> nativizeUuid(col)
                    }
                }.toTypedArray()
            )
            .addDataSourceNames(RedshiftDataTables.REDSHIFT_DATASOURCE_NAME)

        @JvmField
        val IOS_SENSOR_DATA = PostgresTableDefinition(RedshiftDataTables.IOS_SENSOR_DATA.name)
            .addColumns(*IOS_SENSOR_DATA_MIRRORED_COLUMNS.toTypedArray())
            .addColumns(DEDUP_HASH_0, DEDUP_HASH_1)
            .setUnique(DEDUP_HASH_0, DEDUP_HASH_1)
            .addDataSourceNames(RedshiftDataTables.REDSHIFT_DATASOURCE_NAME)

        /**
         * Mirror of the Redshift preprocessed_usage_events table. The upstream preprocessing
         * writer is responsible for dedup, so this table has no dedup hash columns or
         * uniqueness constraints beyond what the writer enforces.
         * run_id is relaxed from varchar(128) to text since the writer imposes no fixed length.
         */
        @JvmField
        val PREPROCESSED_USAGE_EVENTS = PostgresTableDefinition(RedshiftDataTables.PREPROCESSED_USAGE_EVENTS.name)
            .addColumns(
                *RedshiftDataTables.PREPROCESSED_USAGE_EVENTS.columns.map { col ->
                    when (col.name) {
                        "run_id" -> PostgresColumnDefinition("run_id", PostgresDatatype.TEXT)
                        else -> nativizeUuid(col)
                    }
                }.toTypedArray()
            )
            .addDataSourceNames(RedshiftDataTables.REDSHIFT_DATASOURCE_NAME)

        /**
         * Columns used to compute the dedup hash for usage events.
         * All columns except uploaded_at.
         */
        private val USAGE_EVENTS_HASH_KEY_COLUMNS: List<PostgresColumnDefinition> =
            CHRONICLE_USAGE_EVENTS_MIRRORED_COLUMNS.filterNot { it.name == UPLOADED_AT.name }

        /**
         * Column names excluded from the sensor data dedup hash.
         * Name-based (not instance-based) so overridden column types don't break the exclusion.
         * These values legitimately differ between duplicate uploads:
         * sample_id is unreliable, date bounds may vary across invocations.
         */
        private val SENSOR_DATA_EXCLUDED_FROM_HASH: Set<String> = setOf(
            SAMPLE_ID.name,
            START_DATE_TIME.name,
            END_DATE_TIME.name,
            EXACT_RECORDED_DATE_TIME.name
        )

        /**
         * Columns used to compute the dedup hash for iOS sensor data.
         */
        private val SENSOR_DATA_HASH_KEY_COLUMNS: List<PostgresColumnDefinition> =
            IOS_SENSOR_DATA_MIRRORED_COLUMNS.filterNot { it.name in SENSOR_DATA_EXCLUDED_FROM_HASH }

        // ========== Hash & Collision Guard Helpers ==========

        /**
         * Builds the concat_ws expression used as input to hashtextextended.
         * Uses chr(31) (unit separator) as delimiter to avoid ambiguity.
         *
         * @param columns The columns to include in the hash
         * @param tableAlias Optional table alias prefix (e.g. "v" for VALUES subquery)
         * @return SQL expression like: concat_ws(chr(31), v.col1::text, v.col2::text, ...)
         */
        @JvmStatic
        fun buildConcatExpression(
            columns: List<PostgresColumnDefinition>,
            tableAlias: String = ""
        ): String {
            val prefix = if (tableAlias.isNotEmpty()) "$tableAlias." else ""
            val concatArgs = columns.joinToString(", ") { "${prefix}${it.name}::text" }
            return "concat_ws(chr(31), $concatArgs)"
        }

        /**
         * Builds the two salted hash expressions using Postgres built-in hashtextextended.
         *
         * @param columns The columns to include in the hash
         * @param tableAlias Optional table alias prefix
         * @return Pair of SQL expressions: (hashtextextended(..., 0), hashtextextended(..., 1))
         */
        @JvmStatic
        fun buildHashExpressions(
            columns: List<PostgresColumnDefinition>,
            tableAlias: String = ""
        ): Pair<String, String> {
            val concat = buildConcatExpression(columns, tableAlias)
            return "hashtextextended($concat, 0)" to "hashtextextended($concat, 1)"
        }

        /**
         * Builds the WHERE clause that guards against hash collisions on ON CONFLICT DO UPDATE.
         * Uses IS NOT DISTINCT FROM for all columns so NULL = NULL evaluates to true,
         * matching the current COALESCE-based dedup semantics.
         *
         * On a hash collision with different actual data, the WHERE fails and the row is not updated.
         *
         * @param tableName The target table name
         * @param keyColumns The columns that form the logical identity of a row
         * @return SQL WHERE clause fragment
         */
        @JvmStatic
        fun buildCollisionGuard(
            tableName: String,
            keyColumns: List<PostgresColumnDefinition>
        ): String {
            return keyColumns.joinToString("\n  AND ") {
                "$tableName.${it.name} IS NOT DISTINCT FROM EXCLUDED.${it.name}"
            }
        }

        // ========== Upsert SQL Builders ==========

        /**
         * Builds a multiline upsert statement for usage events.
         *
         * Uses INSERT ... SELECT ... FROM (VALUES ...) pattern so each bind parameter is bound
         * once and the dedup hashes are computed in SQL from the selected values via hashtextextended.
         *
         * Bind parameter count per row = number of original columns (same as RedshiftDataTables).
         * Bind parameter ordering is identical to RedshiftDataTables.CHRONICLE_USAGE_EVENTS.columns.
         *
         * @param numLines Number of rows to insert
         * @return SQL string with ? placeholders
         */
        @JvmStatic
        fun buildMultilineUpsertUsageEvents(numLines: Int): String {
            val originalColumns = CHRONICLE_USAGE_EVENTS_MIRRORED_COLUMNS
            check((originalColumns.size * numLines) < MAX_BIND_PARAMETERS) {
                "Maximum number of postgres bind parameters would be exceeded with $numLines lines"
            }

            val tableName = RedshiftDataTables.CHRONICLE_USAGE_EVENTS.name
            val (hash0, hash1) = buildHashExpressions(USAGE_EVENTS_HASH_KEY_COLUMNS, "v")

            // Inner SELECT: compute hashes from VALUES
            val innerSelectCols = originalColumns.joinToString(", ") { "v.${it.name}" } +
                    ",\n           $hash0 AS dh0,\n           $hash1 AS dh1"

            // VALUES (?, ?, ...) with type casts
            val typedParams = originalColumns.joinToString(", ") { "?::${it.datatype.sql()}" }
            val valuesLine = "($typedParams)"
            val valuesLines = (1..numLines).joinToString(",\n        ") { valuesLine }

            // AS v(col1, col2, ...)
            val aliases = originalColumns.joinToString(", ") { it.name }

            // Outer SELECT: dedup within batch via ROW_NUMBER, then insert
            val insertCols = originalColumns.joinToString(", ") { it.name } + ", $DEDUP_HASH_COLUMNS_SQL"
            val outerSelectCols = originalColumns.joinToString(", ") { it.name } + ", dh0, dh1"

            // ON CONFLICT collision guard
            val collisionGuard = buildCollisionGuard(tableName, USAGE_EVENTS_HASH_KEY_COLUMNS)

            return """
                INSERT INTO $tableName ($insertCols)
                SELECT $outerSelectCols FROM (
                    SELECT $innerSelectCols,
                           ROW_NUMBER() OVER (PARTITION BY $hash0, $hash1) AS rn
                    FROM (VALUES
                        $valuesLines
                    ) AS v($aliases)
                ) AS deduped
                WHERE rn = 1
                ON CONFLICT ($DEDUP_HASH_COLUMNS_SQL) DO UPDATE SET ${UPLOADED_AT.name} = EXCLUDED.${UPLOADED_AT.name}
                WHERE $collisionGuard
            """.trimIndent()
        }

        /**
         * Builds a multiline upsert statement for iOS sensor data.
         *
         * On conflict, updates the excluded columns with LEAST/GREATEST semantics:
         * - sample_id: LEAST (keep earliest)
         * - start_date_time: LEAST (keep earliest)
         * - end_date_time: GREATEST (keep latest)
         * - exact_recorded_date_time: LEAST (keep earliest)
         *
         * Bind parameter count and ordering is identical to RedshiftDataTables.IOS_SENSOR_DATA.columns.
         *
         * @param numLines Number of rows to insert
         * @return SQL string with ? placeholders
         */
        @JvmStatic
        fun buildMultilineUpsertSensorEvents(numLines: Int): String {
            val originalColumns = IOS_SENSOR_DATA_MIRRORED_COLUMNS
            check((originalColumns.size * numLines) < MAX_BIND_PARAMETERS) {
                "Maximum number of postgres bind parameters would be exceeded with $numLines lines"
            }

            val tableName = RedshiftDataTables.IOS_SENSOR_DATA.name
            val (hash0, hash1) = buildHashExpressions(SENSOR_DATA_HASH_KEY_COLUMNS, "v")

            // Inner SELECT: compute hashes from VALUES
            val innerSelectCols = originalColumns.joinToString(", ") { "v.${it.name}" } +
                    ",\n           $hash0 AS dh0,\n           $hash1 AS dh1"

            // VALUES (?, ?, ...) with type casts
            val typedParams = originalColumns.joinToString(", ") { "?::${it.datatype.sql()}" }
            val valuesLine = "($typedParams)"
            val valuesLines = (1..numLines).joinToString(",\n        ") { valuesLine }

            // AS v(col1, col2, ...)
            val aliases = originalColumns.joinToString(", ") { it.name }

            // Outer SELECT: dedup within batch via ROW_NUMBER, then insert
            val insertCols = originalColumns.joinToString(", ") { it.name } + ", $DEDUP_HASH_COLUMNS_SQL"
            val outerSelectCols = originalColumns.joinToString(", ") { it.name } + ", dh0, dh1"

            // ON CONFLICT update: LEAST/GREATEST for excluded columns
            val conflictUpdate = listOf(
                "${SAMPLE_ID.name} = LEAST($tableName.${SAMPLE_ID.name}, EXCLUDED.${SAMPLE_ID.name})",
                "${START_DATE_TIME.name} = LEAST($tableName.${START_DATE_TIME.name}, EXCLUDED.${START_DATE_TIME.name})",
                "${END_DATE_TIME.name} = GREATEST($tableName.${END_DATE_TIME.name}, EXCLUDED.${END_DATE_TIME.name})",
                "${EXACT_RECORDED_DATE_TIME.name} = LEAST($tableName.${EXACT_RECORDED_DATE_TIME.name}, EXCLUDED.${EXACT_RECORDED_DATE_TIME.name})"
            ).joinToString(",\n    ")

            // Collision guard
            val collisionGuard = buildCollisionGuard(tableName, SENSOR_DATA_HASH_KEY_COLUMNS)

            return """
                INSERT INTO $tableName ($insertCols)
                SELECT $outerSelectCols FROM (
                    SELECT $innerSelectCols,
                           ROW_NUMBER() OVER (PARTITION BY $hash0, $hash1) AS rn
                    FROM (VALUES
                        $valuesLines
                    ) AS v($aliases)
                ) AS deduped
                WHERE rn = 1
                ON CONFLICT ($DEDUP_HASH_COLUMNS_SQL) DO UPDATE SET
                    $conflictUpdate
                WHERE $collisionGuard
            """.trimIndent()
        }

        /**
         * Builds a multiline INSERT statement for audit events (append-only, no dedup).
         *
         * @param numLines Number of rows to insert
         * @return SQL string with ? placeholders
         */
        @JvmStatic
        fun buildMultilineInsertAuditEvents(numLines: Int): String {
            // Use the local Postgres column types (uuid, not TEXT_UUID) so the bind sites can
            // keep using setString(uuid.toString()) — the ?::uuid cast coerces it server-side.
            val columns = AUDIT.columns.toList()
            check((columns.size * numLines) < MAX_BIND_PARAMETERS) {
                "Maximum number of postgres bind parameters would be exceeded with $numLines lines"
            }

            val tableName = AUDIT.name
            val colNames = columns.joinToString(", ") { it.name }
            val typedParams = columns.joinToString(", ") { "?::${it.datatype.sql()}" }
            val line = "($typedParams)"
            val lines = (1..numLines).joinToString(",\n") { line }

            return "INSERT INTO $tableName ($colNames) VALUES\n$lines"
        }

        // ========== FDW Migration SQL Helpers ==========

        /**
         * Builds SQL to migrate usage events from a Redshift foreign table into the local Postgres table.
         * The hashes are computed in SQL using the same hashtextextended expressions as regular upserts.
         *
         * @param foreignSchema The schema name where the foreign tables are mounted (e.g. "redshift_fdw")
         * @return SQL string with ? placeholders for timestamp range (start, end)
         */
        @JvmStatic
        fun buildFdwMigrationUsageEventsSql(foreignSchema: String): String {
            val originalColumns = CHRONICLE_USAGE_EVENTS_MIRRORED_COLUMNS
            val tableName = RedshiftDataTables.CHRONICLE_USAGE_EVENTS.name
            val foreignTable = "$foreignSchema.$tableName"
            val (hash0, hash1) = buildHashExpressions(USAGE_EVENTS_HASH_KEY_COLUMNS)

            val insertCols = originalColumns.joinToString(", ") { it.name } + ", $DEDUP_HASH_COLUMNS_SQL"
            // Cast varchar(36) -> uuid where the target column was nativized; the foreign
            // table still exposes Redshift's varchar(36).
            val selectCols = originalColumns.joinToString(", ") { col ->
                if (col.datatype == PostgresDatatype.UUID) "${col.name}::uuid" else col.name
            } + ",\n       $hash0,\n       $hash1"
            val collisionGuard = buildCollisionGuard(tableName, USAGE_EVENTS_HASH_KEY_COLUMNS)

            return """
                INSERT INTO $tableName ($insertCols)
                SELECT $selectCols
                FROM $foreignTable
                WHERE ${RedshiftColumns.TIMESTAMP.name} >= ? AND ${RedshiftColumns.TIMESTAMP.name} < ?
                ON CONFLICT ($DEDUP_HASH_COLUMNS_SQL) DO UPDATE SET ${UPLOADED_AT.name} = EXCLUDED.${UPLOADED_AT.name}
                WHERE $collisionGuard
            """.trimIndent()
        }

        /**
         * Builds SQL to migrate iOS sensor data from a Redshift foreign table into the local Postgres table.
         *
         * @param foreignSchema The schema name where the foreign tables are mounted
         * @return SQL string with ? placeholders for timestamp range (start, end)
         */
        @JvmStatic
        fun buildFdwMigrationSensorDataSql(foreignSchema: String): String {
            val originalColumns = IOS_SENSOR_DATA_MIRRORED_COLUMNS
            val tableName = RedshiftDataTables.IOS_SENSOR_DATA.name
            val foreignTable = "$foreignSchema.$tableName"
            val (hash0, hash1) = buildHashExpressions(SENSOR_DATA_HASH_KEY_COLUMNS)

            val insertCols = originalColumns.joinToString(", ") { it.name } + ", $DEDUP_HASH_COLUMNS_SQL"
            val selectCols = originalColumns.joinToString(", ") { col ->
                if (col.datatype == PostgresDatatype.UUID) "${col.name}::uuid" else col.name
            } + ",\n       $hash0,\n       $hash1"

            val conflictUpdate = listOf(
                "${SAMPLE_ID.name} = LEAST($tableName.${SAMPLE_ID.name}, EXCLUDED.${SAMPLE_ID.name})",
                "${START_DATE_TIME.name} = LEAST($tableName.${START_DATE_TIME.name}, EXCLUDED.${START_DATE_TIME.name})",
                "${END_DATE_TIME.name} = GREATEST($tableName.${END_DATE_TIME.name}, EXCLUDED.${END_DATE_TIME.name})",
                "${EXACT_RECORDED_DATE_TIME.name} = LEAST($tableName.${EXACT_RECORDED_DATE_TIME.name}, EXCLUDED.${EXACT_RECORDED_DATE_TIME.name})"
            ).joinToString(",\n    ")

            val collisionGuard = buildCollisionGuard(tableName, SENSOR_DATA_HASH_KEY_COLUMNS)

            return """
                INSERT INTO $tableName ($insertCols)
                SELECT $selectCols
                FROM $foreignTable
                WHERE ${RedshiftColumns.RECORDED_DATE_TIME.name} >= ? AND ${RedshiftColumns.RECORDED_DATE_TIME.name} < ?
                ON CONFLICT ($DEDUP_HASH_COLUMNS_SQL) DO UPDATE SET
                    $conflictUpdate
                WHERE $collisionGuard
            """.trimIndent()
        }

        // ========== Postgres-compatible participant stats queries ==========

        const val UNIQUE_DATES = RedshiftDataTables.UNIQUE_DATES

        val participantStatsIosSql = """
                SELECT ${RedshiftColumns.STUDY_ID.name}, ${RedshiftColumns.PARTICIPANT_ID.name}, string_agg(distinct (${RedshiftColumns.RECORDED_DATE_TIME.name} at time zone ${RedshiftColumns.TIMEZONE.name})::date::text, ',') as $UNIQUE_DATES
                FROM ${RedshiftDataTables.IOS_SENSOR_DATA.name}
                WHERE ${RedshiftColumns.STUDY_ID.name} = ?
                GROUP BY ${RedshiftColumns.STUDY_ID.name}, ${RedshiftColumns.PARTICIPANT_ID.name}
            """.trimIndent()

        val participantStatsAndroidSql = """
                SELECT ${RedshiftColumns.STUDY_ID.name}, ${RedshiftColumns.PARTICIPANT_ID.name}, string_agg(distinct (${RedshiftColumns.TIMESTAMP.name} at time zone ${RedshiftColumns.TIMEZONE.name})::date::text, ',') as $UNIQUE_DATES
                FROM ${RedshiftDataTables.CHRONICLE_USAGE_EVENTS.name}
                WHERE ${RedshiftColumns.STUDY_ID.name} = ? AND timezone != ''
                GROUP BY ${RedshiftColumns.STUDY_ID.name}, ${RedshiftColumns.PARTICIPANT_ID.name}
            """.trimIndent()

        // ========== Column Index Helpers ==========

        private val INSERT_USAGE_EVENT_COL_INDICES: Map<String, Int> by lazy {
            RedshiftDataTables.CHRONICLE_USAGE_EVENTS.columns
                .mapIndexed { index, col -> col.name to index + 1 }.toMap()
        }

        private val INSERT_SENSOR_DATA_COL_INDICES: Map<String, Int> by lazy {
            RedshiftDataTables.IOS_SENSOR_DATA.columns
                .mapIndexed { index, col -> col.name to index + 1 }.toMap()
        }

        @JvmStatic
        fun getInsertUsageEventColumnIndex(col: PostgresColumnDefinition): Int {
            return INSERT_USAGE_EVENT_COL_INDICES.getValue(col.name)
        }

        @JvmStatic
        fun getInsertUsageEventColumnIndex(columnName: String): Int {
            return INSERT_USAGE_EVENT_COL_INDICES.getValue(columnName)
        }

        @JvmStatic
        fun getInsertSensorDataColumnIndex(col: PostgresColumnDefinition): Int {
            return INSERT_SENSOR_DATA_COL_INDICES.getValue(col.name)
        }

        init {
            CHRONICLE_USAGE_EVENTS.addIndexes(
                PostgresColumnsIndexDefinition(CHRONICLE_USAGE_EVENTS, STUDY_ID, PARTICIPANT_ID).ifNotExists(),
                // Explicit short name: the auto-generated name would exceed Postgres's 63-byte
                // identifier limit and get silently truncated, risking drift with manually-created indices.
                PostgresColumnsIndexDefinition(CHRONICLE_USAGE_EVENTS, STUDY_ID, PARTICIPANT_ID, TIMESTAMP)
                    .name("chronicle_usage_events_study_id_participant_id_ts_idx")
                    .ifNotExists()
            )
            IOS_SENSOR_DATA.addIndexes(
                PostgresColumnsIndexDefinition(IOS_SENSOR_DATA, STUDY_ID, PARTICIPANT_ID).ifNotExists(),
                PostgresColumnsIndexDefinition(IOS_SENSOR_DATA, STUDY_ID, PARTICIPANT_ID, RECORDED_DATE_TIME).ifNotExists()
            )
            PREPROCESSED_USAGE_EVENTS.addIndexes(
                PostgresColumnsIndexDefinition(PREPROCESSED_USAGE_EVENTS, STUDY_ID, PARTICIPANT_ID).ifNotExists(),
                // Explicit short name: the auto-generated name would exceed Postgres's 63-byte
                // identifier limit and get silently truncated.
                PostgresColumnsIndexDefinition(PREPROCESSED_USAGE_EVENTS, STUDY_ID, PARTICIPANT_ID, APP_DATETIME_START)
                    .name("preprocessed_usage_events_study_id_participant_id_start_idx")
                    .ifNotExists()
            )
        }
    }
}
