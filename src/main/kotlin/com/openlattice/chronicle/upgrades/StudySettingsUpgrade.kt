package com.openlattice.chronicle.upgrades

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.geekbeast.hazelcast.PreHazelcastUpgradeService
import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.StatementHolderSupplier
import com.openlattice.chronicle.notifications.StudyNotificationSettings
import com.openlattice.chronicle.organizations.ChronicleDataCollectionSettings
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.sensorkit.SensorSetting
import com.openlattice.chronicle.sensorkit.SensorType
import com.openlattice.chronicle.settings.AppUsageFrequency
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.FILTERED_APPS
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.STUDIES
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.SYSTEM_APPS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SETTINGS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.RedshiftColumns
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.study.*
import com.openlattice.chronicle.survey.SurveySettings
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.util.*

/**
 * Adds in survey settings to study settings.
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class StudySettingsUpgrade(
    private val storageResolver: StorageResolver,
    private val upgradeService: UpgradeService,
) : PreHazelcastUpgradeService {

    init {
        upgradeService.registerUpgrade(this)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(StudySettingsUpgrade::class.java)
        private val mapper: ObjectMapper = ObjectMappers.newJsonMapper()

        /**
         * Queries for legacy study settings by id.
         */
        private val LOAD_STUDY_SETTINGS_SQL = "SELECT ${STUDY_ID.name}, ${SETTINGS.name} FROM ${STUDIES.name}"
        private val UPDATE_STUDY_SETTINGS_SQL =
            "UPDATE ${STUDIES.name} SET ${SETTINGS.name} = ? WHERE ${STUDY_ID.name} = ?"
    }

    override fun runUpgrade() {
        try {
            doUpgrade()
        } catch (ex: Exception) {
            upgradeService.failUpgrade(this)
            throw ex
        }
    }

    private fun doUpgrade() {
        if (upgradeService.isUpgradeComplete(this)) {
            return
        }

        val studySettings = BasePostgresIterable(
            StatementHolderSupplier(
                storageResolver.getPlatformStorage(),
                LOAD_STUDY_SETTINGS_SQL
            )
        ) {
            it.getObject(
                STUDY_ID.name,
                UUID::class.java
            ) to mapper.readValue<StudySettings>(it.getString(SETTINGS.name))
        }.toMap()

        val defaultSetting = mapOf(StudySettingType.Survey to SurveySettings())
        storageResolver.getPlatformStorage().connection.use { connection ->
            connection.autoCommit = false

            val count = connection.prepareStatement(UPDATE_STUDY_SETTINGS_SQL).use { ps ->
                studySettings.forEach { studyId, studySettings ->
                    ps.setString(1, mapper.writeValueAsString(StudySettings(studySettings + defaultSetting)))
                    ps.setObject(2, studyId)
                    ps.addBatch()
                }
                ps.executeBatch().sum()
            }

            upgradeService.completeUpgrade(connection, this)
            connection.commit()
            connection.autoCommit = false
            logger.info("Upgraded $count studies")
        }
    }
}
