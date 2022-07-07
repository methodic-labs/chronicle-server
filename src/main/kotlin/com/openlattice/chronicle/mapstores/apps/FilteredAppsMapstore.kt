package com.openlattice.chronicle.mapstores.apps

import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.PostgresArrays.getTextArray
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.geekbeast.postgres.streams.StatementHolderSupplier
import com.geekbeast.rhizome.KotlinDelegatedStringSet
import com.geekbeast.rhizome.mapstores.TestableSelfRegisteringMapStore
import com.hazelcast.config.MapConfig
import com.hazelcast.config.MapStoreConfig
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.FILTERED_APPS
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.STUDIES
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.RedshiftColumns
import com.zaxxer.hikari.HikariDataSource
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import java.sql.ResultSet
import java.sql.SQLException
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@getmethodic.com&gt;
 */
class FilteredAppsMapstore(
    private val hds: HikariDataSource,
) : TestableSelfRegisteringMapStore<UUID, KotlinDelegatedStringSet> {
    companion object {
        const val APP_PACKAGE_NAMES = "app_package_names"
        private val LOAD_ALL_KEYS_SQL = """
            SELECT ${STUDY_ID.name} FROM ${STUDIES.name}
        """.trimIndent()

        private val LOAD_SQL = """
            SELECT ${STUDY_ID.name}, array_agg(${RedshiftColumns.APP_PACKAGE_NAME.name}) as $APP_PACKAGE_NAMES 
            FROM ${FILTERED_APPS.name} WHERE ${STUDY_ID.name} = ANY(?)
            GROUP BY ${STUDY_ID.name}
        """

        private val logger = LoggerFactory.getLogger(FilteredAppsMapstore::class.java)
    }

    private val mapStoreConfig = MapStoreConfig()
        .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
        .setImplementation(this)
        .setEnabled(true)
        .setWriteDelaySeconds(0)

    override fun load(key: UUID): KotlinDelegatedStringSet? {
        return loadAll(listOf(key)).values.firstOrNull()
    }

    override fun loadAll(keys: Collection<UUID>): Map<UUID, KotlinDelegatedStringSet> {
        return BasePostgresIterable(PreparedStatementHolderSupplier(hds, LOAD_SQL) {
            it.setArray(1, PostgresArrays.createUuidArray(it.connection, keys))
        }) {
            ResultSetAdapters.studyId(it) to KotlinDelegatedStringSet(getTextArray(it, APP_PACKAGE_NAMES).toSet())
        }.toMap()
    }

    override fun loadAllKeys(): Iterable<UUID> {
        return BasePostgresIterable(
            StatementHolderSupplier(hds, LOAD_ALL_KEYS_SQL, 50000, false, 0L)
        ) { rs: ResultSet ->
            try {
                ResultSetAdapters.studyId(rs)
            } catch (e: SQLException) {
                logger.error("Unable to read keys for map {}.", mapName, e)
                throw IllegalStateException("Unable to read keys.", e)
            }
        }
    }

    override fun getMapConfig(): MapConfig {
        return MapConfig(mapName).setMapStoreConfig(getMapStoreConfig())
    }

    override fun getMapStoreConfig(): MapStoreConfig = mapStoreConfig

    override fun getMapName(): String = "FILTERED_APPS"


    override fun getTable(): String = FILTERED_APPS.name

    override fun generateTestKey(): UUID = UUID.randomUUID()

    override fun generateTestValue(): KotlinDelegatedStringSet =
        KotlinDelegatedStringSet(setOf(RandomStringUtils.random(5, RandomStringUtils.random(5))))

    override fun deleteAll(keys: MutableCollection<UUID>) {
        throw UnsupportedOperationException("The Study Mapstore is a READ ONLY cache.")
    }

    override fun delete(key: UUID) {
        throw UnsupportedOperationException("The Study Mapstore is a READ ONLY cache.")
    }

    override fun storeAll(map: MutableMap<UUID, KotlinDelegatedStringSet>) {
        throw UnsupportedOperationException("The Study Mapstore is a READ ONLY cache.")
    }

    override fun store(key: UUID, value: KotlinDelegatedStringSet) {
        throw UnsupportedOperationException("The Study Mapstore is a READ ONLY cache.")
    }

}