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
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.ANDROID_APPS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APPLICATION_LABEL
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_PACKAGE_NAME
import com.zaxxer.hikari.HikariDataSource
import org.apache.commons.lang3.RandomStringUtils
import org.springframework.stereotype.Component
import java.sql.ResultSet

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */

@Component
class AndroidAppsMapstore(val hds: HikariDataSource) :
    TestableSelfRegisteringMapStore<String, String> {
    private val mapStoreConfig = MapStoreConfig()
        .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
        .setImplementation(this)
        .setEnabled(true)
        .setWriteDelaySeconds(0)

    private val mapConfig = MapConfig(mapName).setMapStoreConfig(mapStoreConfig)

    companion object {
        private const val FETCH_SIZE = 65536

        private val LOAD_KEYS_SQL = """
            SELECT ${APP_PACKAGE_NAME.name} FROM ${ANDROID_APPS.name}
        """.trimIndent()

        private val GET_ANDROID_APPS_SQL = """
            SELECT ${APP_PACKAGE_NAME.name}, ${APPLICATION_LABEL.name}
            FROM ${ANDROID_APPS.name}
            WHERE ${APP_PACKAGE_NAME.name} = ANY(?)
        """.trimIndent()

        private val INSERT_SQL = """
            INSERT INTO ${ANDROID_APPS.name} (${APP_PACKAGE_NAME.name}, ${APPLICATION_LABEL.name })
            VALUES(?, ?)
            ON CONFLICT DO NOTHING
        """.trimIndent()
    }

    private fun mapKey(rs: ResultSet): String {
        return ResultSetAdapters.appPackageName(rs)
    }

    private fun mapValue(rs: ResultSet): String {
        return ResultSetAdapters.applicationLabel(rs)
    }

    override fun load(appPackageName: String): String? {
        return loadAll(listOf(appPackageName)).values.firstOrNull()
    }

    override fun loadAll(appPackageNames: Collection<String>): Map<String, String> {
        return BasePostgresIterable(
            PreparedStatementHolderSupplier(hds, GET_ANDROID_APPS_SQL) { ps ->
                ps.setArray(1, PostgresArrays.createTextArray(ps.connection, appPackageNames))
            }
        ) { mapKey(it) to mapValue(it) }.toMap()
    }

    override fun loadAllKeys(): Iterable<String> {
        return BasePostgresIterable(
            StatementHolderSupplier(hds, LOAD_KEYS_SQL, fetchSize = FETCH_SIZE)
        ) { mapKey(it) }
    }

    override fun store(appPackageName: String, applicationLabel: String) {
        storeAll(mapOf(appPackageName to applicationLabel))
    }

    override fun storeAll(map: Map<String, String>) {
        hds.connection.use { connection ->
            connection.prepareStatement(INSERT_SQL).use { ps ->
                map.forEach { (appPackageName, applicationLabel) ->
                    ps.setString(1, appPackageName)
                    ps.setString(2, applicationLabel)
                    ps.setString(3, applicationLabel)
                    ps.addBatch()
                }
                ps.executeBatch()
            }
        }
    }

    override fun delete(key: String?) {
        throw UnsupportedOperationException("Android apps Map store does not allow deletion")
    }

    override fun deleteAll(keys: MutableCollection<String>?) {
        throw UnsupportedOperationException("Android apps Map store does not allow deletion")
    }

    override fun getMapConfig(): MapConfig {
        return mapConfig
    }

    override fun getMapStoreConfig(): MapStoreConfig {
        return mapStoreConfig
    }

    override fun getMapName(): String {
        return HazelcastMap.ANDROID_APPS.name
    }

    override fun getTable(): String {
        return ANDROID_APPS.name
    }

    override fun generateTestKey(): String {
        return RandomStringUtils.randomAlphabetic(10)
    }

    override fun generateTestValue(): String {
        return RandomStringUtils.randomAlphabetic(10)
    }
}