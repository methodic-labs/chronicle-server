package com.openlattice.chronicle.storage

import com.openlattice.postgres.PostgresTableDefinition

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresDataTables {
    companion object {
        const val POSTGRES_DATA_ENVIRONMENT = "postgres_data"

        @JvmStatic
        val CHRONICLE_USAGE_EVENTS = PostgresTableDefinition("chronicle_usage_events")
            .addColumns(*RedshiftDataTables.CHRONICLE_USAGE_EVENTS.columns.toTypedArray())
            .primaryKey(*RedshiftDataTables.CHRONICLE_USAGE_EVENTS.columns.toTypedArray())

        @JvmStatic
        val CHRONICLE_USAGE_STATS = PostgresTableDefinition("chronicle_usage_stats")
            .addColumns(*RedshiftDataTables.CHRONICLE_USAGE_STATS.columns.toTypedArray())
            .primaryKey(*RedshiftDataTables.CHRONICLE_USAGE_EVENTS.columns.toTypedArray())
    }
}