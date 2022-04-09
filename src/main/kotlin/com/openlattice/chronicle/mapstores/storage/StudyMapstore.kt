package com.openlattice.chronicle.mapstores.storage

import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.geekbeast.postgres.streams.StatementHolderSupplier
import com.geekbeast.rhizome.mapstores.TestableSelfRegisteringMapStore
import com.hazelcast.config.MapConfig
import com.hazelcast.config.MapStoreConfig
import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.services.studies.StudyService.Companion.GET_STUDIES_SQL
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.ORGANIZATION_STUDIES
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.STUDIES
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import com.openlattice.chronicle.study.Study
import com.openlattice.chronicle.util.tests.TestDataFactory
import com.zaxxer.hikari.HikariDataSource
import org.springframework.stereotype.Component
import java.sql.ResultSet
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class StudyMapstore(val hds: HikariDataSource) : TestableSelfRegisteringMapStore<UUID, Study> {
    private val mapStoreConfig = MapStoreConfig()
        .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
        .setImplementation(this)
        .setEnabled(true)
        .setWriteDelaySeconds(0)

    private val mapConfig = MapConfig(mapName).setMapStoreConfig(mapStoreConfig)

    override fun getMapConfig(): MapConfig = mapConfig
    override fun getMapStoreConfig(): MapStoreConfig = mapStoreConfig

    companion object {

        private val LOAD_KEYS_SQL = """
            SELECT ${STUDY_ID.name} FROM ${STUDIES.name} 
        """.trimIndent()

    }

    private fun mapKey(rs: ResultSet): UUID = ResultSetAdapters.studyId(rs)

    private fun mapValue(rs: ResultSet): Study = ResultSetAdapters.study(rs)

    override fun generateTestKey(): UUID = UUID.randomUUID()
    override fun generateTestValue(): Study = TestDataFactory.study()

    override fun load(key: UUID): Study? {
        return loadAll(listOf(key)).values.firstOrNull()
    }

    override fun loadAll(keys: Collection<UUID>): Map<UUID, Study> {
        return BasePostgresIterable(PreparedStatementHolderSupplier(hds, GET_STUDIES_SQL) { ps ->
            ps.setArray(1, PostgresArrays.createUuidArray(ps.connection, keys))
        }) { mapKey(it) to mapValue(it) }.toMap()
    }

    override fun loadAllKeys(): Iterable<UUID> {
        return BasePostgresIterable(StatementHolderSupplier(hds, LOAD_KEYS_SQL, fetchSize = 65536)) { mapKey(it) }
    }

    override fun store(key: UUID, value: Study) {
        throw UnsupportedOperationException("The Study Mapstore is a READ ONLY cache.")
    }

    override fun storeAll(map: Map<UUID, Study>) {
        throw UnsupportedOperationException("The Study Mapstore is a READ ONLY cache.")
    }

    override fun delete(key: UUID) {
         throw UnsupportedOperationException("The Study Mapstore is a READ ONLY cache.")
    }

    override fun deleteAll(keys: MutableCollection<UUID>) {
        throw UnsupportedOperationException("The Study Mapstore is a READ ONLY cache.")
    }

    override fun getMapName(): String = HazelcastMap.STUDIES.name


    override fun getTable(): String = STUDIES.name
}
