package com.openlattice.chronicle.storage

import com.geekbeast.configuration.postgres.PostgresFlavor
import com.openlattice.chronicle.configuration.ChronicleStorageConfiguration
import com.geekbeast.jdbc.DataSourceManager
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

    fun resolve(studyId: UUID, requiredFlavor: PostgresFlavor = PostgresFlavor.REDSHIFT): HikariDataSource {
        val (flavor, hds) = resolveAndGetFlavor(studyId)
        check(flavor == PostgresFlavor.ANY || flavor == requiredFlavor) { "Configured flavor $flavor does not much required flavor $requiredFlavor" }
        return hds
    }

    fun resolveAndGetFlavor(studyId: UUID): Pair<PostgresFlavor, HikariDataSource> {
        return getDataSource(resolveDataSourceName(studyId))
    }

    fun resolveDataSourceName(studyId: UUID): String {
        return datasourceMappings[studyId] ?: storageConfiguration.defaultStorage
    }

    fun getStudyIdsByDataSourceName(studyIds: Collection<UUID>): Map<String, List<UUID>> {
        return studyIds.groupBy { resolveDataSourceName(it) }
    }

    fun getDataSource(dataSourceName: String): Pair<PostgresFlavor, HikariDataSource> {
        return dataSourceManager.getFlavor(dataSourceName) to dataSourceManager.getDataSource(dataSourceName)
    }

    fun getAuditStorage(): Pair<PostgresFlavor, HikariDataSource> {
        return with(dataSourceManager) {
            getFlavor(storageConfiguration.auditStorage) to getDataSource(storageConfiguration.auditStorage)
        }
    }

    fun getPlatformStorage(requiredFlavor: PostgresFlavor = PostgresFlavor.VANILLA): HikariDataSource {
        val (flavor, hds) = getDefaultPlatformStorage()
        check(flavor == PostgresFlavor.ANY || flavor == requiredFlavor) { "Configured flavor $flavor does not much required flavor $requiredFlavor" }
        return hds
    }

    fun getDefaultPlatformStorage(): Pair<PostgresFlavor, HikariDataSource> {
        return with(dataSourceManager) {
            getFlavor(storageConfiguration.platformStorage) to getDataSource(storageConfiguration.platformStorage)
        }
    }

    fun getDefaultStorage(): Pair<PostgresFlavor, HikariDataSource> {
        return with(dataSourceManager) {
            getFlavor(storageConfiguration.defaultStorage) to getDataSource(storageConfiguration.defaultStorage)
        }
    }

}

