package com.openlattice.chronicle.storage

import com.geekbeast.configuration.postgres.PostgresFlavor
import com.openlattice.chronicle.configuration.ChronicleStorageConfiguration
import com.geekbeast.jdbc.DataSourceManager
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.openlattice.chronicle.hazelcast.processors.storage.StudyStorageRead
import com.openlattice.chronicle.hazelcast.processors.storage.StudyStorageUpdate
import com.openlattice.chronicle.study.Study
import com.zaxxer.hikari.HikariDataSource
import org.springframework.stereotype.Component
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class StorageResolver constructor(
    private val dataSourceManager: DataSourceManager,
    private val storageConfiguration: ChronicleStorageConfiguration
) {
    private lateinit var studyStorage: IMap<UUID, Study>

    fun associateStudyWithStorage(studyId: UUID, storage: String = ChronicleStorage.CHRONICLE.id) {
        studyStorage.executeOnKey(studyId, StudyStorageUpdate(storage))
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
        return studyStorage.executeOnKey(studyId, StudyStorageRead()) ?: storageConfiguration.defaultEventStorage
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

    fun setStudyStorage( hazelcastInstance: HazelcastInstance ) {
        studyStorage = HazelcastMap.STUDIES.getMap(hazelcastInstance)
    }
}

