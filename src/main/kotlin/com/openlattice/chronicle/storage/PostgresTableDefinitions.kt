package com.openlattice.chronicle.storage

import com.openlattice.postgres.PostgresTableDefinition
import com.openlattice.postgres.RedshiftTableDefinition

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresTables {
    companion object {
        val CHRONICLE_USAGE_EVENTS = PostgresTableDefinition("chronicle_usage_events")
                .addColumns(*RedshiftTables.CHRONICLE_USAGE_EVENTS.columns.toTypedArray())

        val CHRONICLE_USAGE_STATS = PostgresTableDefinition("chronicle_usage_stats")
                .addColumns(*RedshiftTables.CHRONICLE_USAGE_STATS.columns.toTypedArray())
    }


}