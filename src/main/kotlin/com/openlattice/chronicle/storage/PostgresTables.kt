package com.openlattice.chronicle.storage

import com.openlattice.postgres.PostgresTableDefinition

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresTables {
    companion object {
        const val POSTGRES_DATA_ENVIRONMENT = "postgres_data"
        val ORGANIZATIONS = PostgresTableDefinition("organizations")
                .addColumns(
                        PostgresColumn.ID,
                        PostgresColumn.NULLABLE_TITLE,
                        PostgresColumn.DESCRIPTION,
                        PostgresColumn.ALLOWED_EMAIL_DOMAINS,
                        PostgresColumn.MEMBERS,
                        PostgresColumn.APP_IDS,
                        PostgresColumn.PARTITIONS,
                        PostgresColumn.ORGANIZATION
                )
                .overwriteOnConflict()
        val STUDIES = PostgresTableDefinition("studies")
                .addColumns(*RedshiftDataTables.CHRONICLE_USAGE_STATS.columns.toTypedArray())
                .primaryKey(*RedshiftDataTables.CHRONICLE_USAGE_EVENTS.columns.toTypedArray())

    }
}