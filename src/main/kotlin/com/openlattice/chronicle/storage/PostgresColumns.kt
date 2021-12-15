package com.openlattice.chronicle.storage

import com.openlattice.postgres.PostgresColumnDefinition
import com.openlattice.postgres.PostgresDatatype

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresColumns {
    companion object {
        val ID = PostgresColumnDefinition("id", PostgresDatatype.UUID).primaryKey().notNull()
    }
}