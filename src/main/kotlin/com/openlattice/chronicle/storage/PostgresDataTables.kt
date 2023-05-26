package com.openlattice.chronicle.storage

import com.geekbeast.postgres.PostgresTableDefinition

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
            .addDataSourceNames(RedshiftDataTables.REDSHIFT_DATASOURCE_NAME)

        @JvmField
        val CHRONICLE_USAGE_STATS = PostgresTableDefinition(RedshiftDataTables.CHRONICLE_USAGE_STATS.name)
            .addColumns(*RedshiftDataTables.CHRONICLE_USAGE_STATS.columns.toTypedArray())
            .primaryKey(*RedshiftDataTables.CHRONICLE_USAGE_STATS.columns.toTypedArray())
            .addDataSourceNames(RedshiftDataTables.REDSHIFT_DATASOURCE_NAME)

        @JvmField
        val AUDIT = PostgresTableDefinition(RedshiftDataTables.AUDIT.name)
            .addColumns(*RedshiftDataTables.AUDIT.columns.toTypedArray())
            .primaryKey(*RedshiftDataTables.AUDIT.columns.toTypedArray())
            .addDataSourceNames(RedshiftDataTables.REDSHIFT_DATASOURCE_NAME)

        @JvmField
        val IOS_SENSOR_DATA = PostgresTableDefinition(RedshiftDataTables.IOS_SENSOR_DATA.name)
            .addColumns(*RedshiftDataTables.IOS_SENSOR_DATA.columns.toTypedArray())
            .primaryKey(*RedshiftDataTables.IOS_SENSOR_DATA.columns.toTypedArray())
            .addDataSourceNames(RedshiftDataTables.REDSHIFT_DATASOURCE_NAME)

    }
}