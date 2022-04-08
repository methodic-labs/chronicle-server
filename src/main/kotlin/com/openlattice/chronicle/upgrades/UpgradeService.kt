package com.openlattice.chronicle.upgrades

import com.fasterxml.jackson.databind.ObjectMapper
import com.geekbeast.hazelcast.PreHazelcastUpgradeService
import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.StatementHolderSupplier
import com.openlattice.chronicle.organizations.ChronicleDataCollectionSettings
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.sensorkit.SensorSetting
import com.openlattice.chronicle.sensorkit.SensorType
import com.openlattice.chronicle.settings.AppUsageFrequency
import com.openlattice.chronicle.storage.ChroniclePostgresTables
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.STUDIES
import com.openlattice.chronicle.storage.PostgresColumns.Companion.MODULES
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SETTINGS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.study.*
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class UpgradeService(private val storageResolver: StorageResolver) : PreHazelcastUpgradeService {

    companion object {
        private val logger = LoggerFactory.getLogger(UpgradeService::class.java)
        private val mapper: ObjectMapper = ObjectMappers.newJsonMapper()

        /**
         * Queries for legacy study settings by id.
         */
        private val ADD_COLUMN_SQL = """
            ALTER TABLE ${STUDIES.name} ADD COLUMN IF NOT EXISTS ${MODULES.name} ${MODULES.datatype.sql()} DEFAULT '{}'
        """.trimIndent()
        private val LEGACY_STUDY_SETTINGS_SQL = """
                 SELECT ${STUDY_ID.name},${SETTINGS.name} FROM ${STUDIES.name}
                   WHERE ${SETTINGS.name} ? 'appUsageFrequency'
            """.trimIndent()
        private val UPDATE_LEGACY_STUDY = """
            UPDATE ${STUDIES.name} SET ${SETTINGS.name} = ?::jsonb, ${MODULES.name} = ?::jsonb 
            WHERE ${STUDY_ID.name} = ?
         """.trimIndent()

        private val GET_STUDY_IDS_SQL = """
            SELECT ${STUDY_ID.name} FROM ${STUDIES.name}
        """.trimIndent()

        /**
         * 1. STUDY_ID
         * 2. PARTICIPANT_LIMIT
         * 3. STUDY_DURATION
         * 4. DATA_RETENTION
         * 5. FEATURES
         */
        private val INSERT_STUDY_LIMITS = """
            INSERT INTO ${ChroniclePostgresTables.STUDY_LIMITS.name} VALUES(?,?,?::jsonb,?::jsonb,?) 
        """.trimIndent()

    }

    override fun runUpgrade() {
        val legacySettings = BasePostgresIterable(
            StatementHolderSupplier(storageResolver.getPlatformStorage(), LEGACY_STUDY_SETTINGS_SQL)
        ) {
            ResultSetAdapters.legacyStudySettings(it)
        }.toMap()
        if( legacySettings.isEmpty() ) {
            return
        }
        val studyIds = BasePostgresIterable(
            StatementHolderSupplier(storageResolver.getPlatformStorage(), GET_STUDY_IDS_SQL)
        ) { ResultSetAdapters.studyId(it) }.toList()
        val modulesMap = mutableMapOf<UUID, Map<StudyFeature, Any>>()
        storageResolver.getPlatformStorage().connection.use { connection ->
            connection.autoCommit = false
            connection.createStatement().use { s -> s.execute(ADD_COLUMN_SQL) }
            val upgradedCount = connection.prepareStatement(UPDATE_LEGACY_STUDY).use { ps ->
                legacySettings.forEach { (studyId, settings) ->
                    modulesMap[studyId] = migrateComponents(settings)
                    val upgradeSettings = StudySettings(
                        mapOf(
                            migrateDataCollectionSettings(settings),
                            migrateSensorSettings(settings)
                        )
                    )

                    ps.setString(1, mapper.writeValueAsString(upgradeSettings))
                    ps.setString(2, mapper.writeValueAsString(modulesMap.getValue(studyId)))
                    ps.setObject(3, studyId)
                    ps.addBatch()
                }
                ps.executeBatch().sum()
            }

            upgradeStudyLimits(connection, studyIds)
            connection.commit()
            logger.info("Upgrade $upgradedCount studies.")
        }
    }

    private fun upgradeStudyLimits(connection: Connection, studyIds: List<UUID>) {
        val studyLimits = StudyLimits(
            StudyDuration(Short.MAX_VALUE),
            StudyDuration(Short.MAX_VALUE), Int.MAX_VALUE,
            EnumSet.allOf(StudyFeature::class.java)
        )

        connection.prepareStatement(INSERT_STUDY_LIMITS).use { ps ->
            studyIds.forEach { studyId ->
                ps.setObject(1, studyId)
                ps.setInt(2, studyLimits.participantLimit)
                ps.setString(3, mapper.writeValueAsString(studyLimits.studyDuration))
                ps.setString(4, mapper.writeValueAsString(studyLimits.dataRetentionDuration))
                ps.setArray(5, PostgresArrays.createTextArray(connection, studyLimits.features.map { it.name }))
                ps.addBatch()
            }
            val count = ps.executeBatch().sum()
            logger.info("Upgrade $count study limits.")
        }
    }

    private fun migrateComponents(settings: Map<String, Any>): Map<StudyFeature, Any> {
        return (settings["components"] as Collection<*>? ?: listOf<Any>())
            .filterNotNull()
            .map { StudyFeature.valueOf(it as String) }
            .associateWith { emptyMap<StudyFeature, Any>() }
    }

    private fun migrateSensorSettings(settings: Map<String, Any>): Pair<StudySettingType, StudySetting> {
        val sensors = settings["sensors"]
        return StudySettingType.Sensor to SensorSetting(
            when (sensors) {
                null -> setOf()
                is Set<*> -> sensors.mapNotNull { SensorType.valueOf(it as String) }.toSet()
                else -> throw IllegalStateException("Unexpected type encountered.")
            }
        )
    }

    private fun migrateDataCollectionSettings(settings: Map<String, Any>): Pair<StudySettingType, StudySetting> {
        val appUsageFrequency = settings["appUsageFrequency"]
        return StudySettingType.DataCollection to ChronicleDataCollectionSettings(
            when (appUsageFrequency) {
                null -> AppUsageFrequency.HOURLY
                is String -> AppUsageFrequency.valueOf(appUsageFrequency)
                else -> throw IllegalStateException("Unexpected type encountered.")
            }
        )
    }
}