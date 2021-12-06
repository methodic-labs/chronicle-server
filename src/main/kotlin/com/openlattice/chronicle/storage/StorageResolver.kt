package com.openlattice.chronicle.storage

import com.geekbeast.configuration.postgres.PostgresFlavor
import com.openlattice.jdbc.DataSourceManager
import com.zaxxer.hikari.HikariDataSource
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class StorageResolver(
    val dataSourceManager: DataSourceManager,
    val defaultStorage: String
) {
    private val datasourceMappings: Map<UUID, String> = mutableMapOf()
    fun resolve(studyId: UUID ) : Pair<PostgresFlavor, HikariDataSource> {
        val dataSourceName = datasourceMappings[studyId] ?: defaultStorage
        return dataSourceManager.getFlavor(dataSourceName) to dataSourceManager.getDataSource(dataSourceName )
    }
}

