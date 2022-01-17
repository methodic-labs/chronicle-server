package com.openlattice.chronicle.storage

import com.openlattice.postgres.PostgresTableDefinition

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresDataTables {
    companion object {
        const val POSTGRES_DATA_ENVIRONMENT = "postgres_data"

        @JvmField
        val CHRONICLE_USAGE_EVENTS = PostgresTableDefinition(RedshiftDataTables.CHRONICLE_USAGE_EVENTS.name)
            .addColumns(*RedshiftDataTables.CHRONICLE_USAGE_EVENTS.columns.toTypedArray())
            .primaryKey(*RedshiftDataTables.CHRONICLE_USAGE_EVENTS.columns.toTypedArray())

        @JvmField
        val CHRONICLE_USAGE_STATS = PostgresTableDefinition(RedshiftDataTables.CHRONICLE_USAGE_STATS.name)
            .addColumns(*RedshiftDataTables.CHRONICLE_USAGE_STATS.columns.toTypedArray())
            .primaryKey(*RedshiftDataTables.CHRONICLE_USAGE_STATS.columns.toTypedArray())

        @JvmField
        val AUDIT = PostgresTableDefinition(RedshiftDataTables.AUDIT.name)
            .addColumns(*RedshiftDataTables.AUDIT.columns.toTypedArray())
            .primaryKey(*RedshiftDataTables.AUDIT.columns.toTypedArray())
    }
}