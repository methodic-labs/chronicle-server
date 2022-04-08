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
class FixUpgrade(private val storageResolver: StorageResolver) : PreHazelcastUpgradeService {

    companion object {
        private val logger = LoggerFactory.getLogger(FixUpgrade::class.java)
        private val mapper: ObjectMapper = ObjectMappers.newJsonMapper()

        /**
         * Queries for legacy study settings by id.
         */
        private val ADD_COLUMN_SQL = """
            ALTER TABLE ${STUDIES.name} ADD COLUMN IF NOT EXISTS ${MODULES.name} ${MODULES.datatype.sql()} DEFAULT '{}'
        """.trimIndent()
        private val LEGACY_STUDY_SETTINGS_SQL = """
                 SELECT ${STUDY_ID.name},${SETTINGS.name} FROM ${STUDIES.name}
            """.trimIndent()
        private val UPDATE_LEGACY_STUDY = """
            UPDATE ${STUDIES.name} SET ${SETTINGS.name} = ?::jsonb 
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

        storageResolver.getPlatformStorage().connection.use { connection ->
            connection.autoCommit = false
            val upgradedCount = connection.prepareStatement(UPDATE_LEGACY_STUDY).use { ps ->
                legacySettings.forEach { (studyId, settings) ->
                    val upgradeSettings = StudySettings(mapOf(
                        migrateDataCollectionSettings(settings),
                    ))

                    ps.setString(1, mapper.writeValueAsString(upgradeSettings))
                    ps.setObject(2, studyId)
                    ps.addBatch()
                }
                ps.executeBatch().sum()
            }
            connection.commit()
            logger.info("Upgrade $upgradedCount studies.")
        }
    }

    private fun migrateDataCollectionSettings(settings: Map<String, Any>): Pair<StudySettingType, StudySetting> {
        val appUsageFrequency = (settings[StudySettingType.DataCollection.name] as Map<*, *>? ?: mapOf<String,Any>() )["appUsageFrequency"]
        return StudySettingType.DataCollection to ChronicleDataCollectionSettings(
            when (appUsageFrequency) {
                null -> AppUsageFrequency.HOURLY
                is String -> AppUsageFrequency.valueOf(appUsageFrequency)
                else -> throw IllegalStateException("Unexpected type encountered.")
            }
        )
    }
}