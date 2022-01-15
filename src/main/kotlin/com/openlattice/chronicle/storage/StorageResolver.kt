package com.openlattice.chronicle.storage

import com.geekbeast.configuration.postgres.PostgresFlavor
import com.openlattice.chronicle.configuration.ChronicleStorageConfiguration
import com.openlattice.jdbc.DataSourceManager
import com.zaxxer.hikari.HikariDataSource
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class StorageResolver constructor(
    private val dataSourceManager: DataSourceManager,
    private val storageConfiguration: ChronicleStorageConfiguration
) {
    private val datasourceMappings: Map<UUID, String> = mutableMapOf()
    fun resolve(studyId: UUID): Pair<PostgresFlavor, HikariDataSource> {
        val dataSourceName = datasourceMappings[studyId] ?: storageConfiguration.defaultStorage
        return dataSourceManager.getFlavor(dataSourceName) to dataSourceManager.getDataSource(dataSourceName)
    }

    fun getAuditStorage(): Pair<PostgresFlavor, HikariDataSource> {
        return with(dataSourceManager) {
            getFlavor(storageConfiguration.auditStorage) to getDataSource(storageConfiguration.auditStorage)
        }
    }

    fun getAuthorizationStorage(): Pair<PostgresFlavor, HikariDataSource> {
        return with(dataSourceManager) {
            getFlavor(storageConfiguration.authorizationStorage) to getDataSource(storageConfiguration.authorizationStorage)
        }
    }

    fun getDefaultStorage(): Pair<PostgresFlavor, HikariDataSource> {
        return with(dataSourceManager) {
            getFlavor(storageConfiguration.defaultStorage) to getDataSource(storageConfiguration.defaultStorage)
        }
    }
}

