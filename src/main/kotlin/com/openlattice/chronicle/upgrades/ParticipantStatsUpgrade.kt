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
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.PARTICIPANT_STATS
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.STUDIES
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.SYSTEM_APPS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ANDROID_LAST_PING
import com.openlattice.chronicle.storage.PostgresColumns.Companion.IOS_LAST_PING
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.RedshiftColumns
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.UPLOADED_AT
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.CHRONICLE_USAGE_EVENTS
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.study.*
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class ParticipantStatsUpgrade(
    private val storageResolver: StorageResolver,
    private val upgradeService: UpgradeService,
) : PreHazelcastUpgradeService {

    init {
        upgradeService.registerUpgrade(this)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ParticipantStatsUpgrade::class.java)

        private val ADD_ANDROID_LAST_PING_SQL = """
            ALTER TABLE ${PARTICIPANT_STATS.name} ADD COLUMN ${ANDROID_LAST_PING.name} ${ANDROID_LAST_PING.datatype.sql()} 
        """.trimIndent()
        private val ADD_IOS_LAST_PING_SQL = """
            ALTER TABLE ${PARTICIPANT_STATS.name} ADD COLUMN ${IOS_LAST_PING.name} ${IOS_LAST_PING.datatype.sql()} 
        """.trimIndent()

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

        storageResolver.getPlatformStorage().connection.use { connection ->
            connection.autoCommit = false

            connection.createStatement().use { s ->
                s.execute(ADD_ANDROID_LAST_PING_SQL)

                s.execute(ADD_IOS_LAST_PING_SQL)
            }

            upgradeService.completeUpgrade(connection, this)
            connection.commit()

            logger.info("Added android and ios last ping columns.")

            connection.autoCommit = false

        }
    }
}
