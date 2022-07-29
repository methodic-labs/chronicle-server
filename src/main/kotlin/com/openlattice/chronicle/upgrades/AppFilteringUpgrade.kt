package com.openlattice.chronicle.upgrades

import com.fasterxml.jackson.databind.ObjectMapper
import com.geekbeast.hazelcast.PreHazelcastUpgradeService
import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.postgres.PostgresArrays
import com.openlattice.chronicle.notifications.StudyNotificationSettings
import com.openlattice.chronicle.organizations.ChronicleDataCollectionSettings
import com.openlattice.chronicle.sensorkit.SensorSetting
import com.openlattice.chronicle.sensorkit.SensorType
import com.openlattice.chronicle.settings.AppUsageFrequency
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.FILTERED_APPS
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.STUDIES
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.SYSTEM_APPS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.RedshiftColumns
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.study.*
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class AppFilteringUpgrade(
    private val storageResolver: StorageResolver,
    private val upgradeService: UpgradeService,
) : PreHazelcastUpgradeService {

    init {
        upgradeService.registerUpgrade(this)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(AppFilteringUpgrade::class.java)
        private val mapper: ObjectMapper = ObjectMappers.newJsonMapper()

        /**
         * Queries for legacy study settings by id.
         */
        private val RENAME_TABLE_SQL = "ALTER TABLE system_apps RENAME TO ${SYSTEM_APPS.name}"
        private val INITIALIZE_STUDIES_SQL = """
            INSERT INTO ${FILTERED_APPS.name} SELECT ${STUDY_ID.name}, ${RedshiftColumns.APP_PACKAGE_NAME} 
            FROM $STUDIES.name} CROSS JOIN ${SYSTEM_APPS.name}
        """.trimIndent()

    }

    override fun runUpgrade() {
        storageResolver.getPlatformStorage().connection.use { connection ->
            connection.autoCommit = false

            connection.createStatement().use { s -> s.execute(RENAME_TABLE_SQL) }
            logger.info("Renamed default filtered apps table.")
            //Populate studies with default filtered apps
            val count = connection.createStatement().use { s -> s.executeUpdate(INITIALIZE_STUDIES_SQL) }

            connection.commit()
            connection.autoCommit = false
            logger.info("Upgraded $count studies")

        }
    }
}
