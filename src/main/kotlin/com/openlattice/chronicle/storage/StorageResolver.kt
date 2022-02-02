package com.openlattice.chronicle.storage

import com.geekbeast.configuration.postgres.PostgresFlavor
import com.openlattice.chronicle.configuration.ChronicleStorageConfiguration
import com.geekbeast.jdbc.DataSourceManager
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.openlattice.chronicle.configuration.CHRONICLE_STORAGE
import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.zaxxer.hikari.HikariDataSource
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class StorageResolver constructor(
    private val dataSourceManager: DataSourceManager,
    private val storageConfiguration: ChronicleStorageConfiguration,
    hazelcastInstance: HazelcastInstance
) {
    private val studyStorage: IMap<UUID, String> = HazelcastMap.STUDY_STORAGE.getMap(hazelcastInstance)

    fun associateStudyWithStorage(studyId: UUID, storage: String = CHRONICLE_STORAGE) {
        studyStorage[studyId] = storage
    }

    fun resolve(studyId: UUID, requiredFlavor: PostgresFlavor = PostgresFlavor.REDSHIFT): HikariDataSource {
        val (flavor, hds) = resolveAndGetFlavor(studyId)
        check(flavor == PostgresFlavor.ANY || flavor == requiredFlavor) { "Configured flavor $flavor does not much required flavor $requiredFlavor" }
        return hds
    }

    fun resolveAndGetFlavor(studyId: UUID): Pair<PostgresFlavor, HikariDataSource> {
        return getDataSource(resolveDataSourceName(studyId))
    }

    fun resolveDataSourceName(studyId: UUID): String {
        return studyStorage[studyId] ?: storageConfiguration.defaultEventStorage
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

    fun getEventStorageWithFlavor(requiredFlavor: PostgresFlavor = PostgresFlavor.REDSHIFT): HikariDataSource {
        val (flavor, hds) = getDefaultEventStorage()
        check(flavor == PostgresFlavor.ANY || flavor == requiredFlavor) { "Configured flavor $flavor does not much required flavor $requiredFlavor" }
        return hds
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

    fun getDefaultEventStorage(): Pair<PostgresFlavor, HikariDataSource> {
        return with(dataSourceManager) {
            getFlavor(storageConfiguration.defaultEventStorage) to getDataSource(storageConfiguration.defaultEventStorage)
        }
    }
}

