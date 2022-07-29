package com.openlattice.chronicle.upgrades

import com.geekbeast.hazelcast.PreHazelcastUpgradeService
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.UPGRADES
import com.openlattice.chronicle.storage.PostgresColumns.Companion.LAST_UPDATE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.UPGRADE_CLASS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.UPGRADE_STATUS
import com.openlattice.chronicle.storage.StorageResolver
import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection


/**
 * Simple state mechine for upgrades. Good candidate for moving into rhizome.
 * @author Matthew Tamayo-Rios &lt;matthew@getmethodic.com&gt;
 */
class UpgradeService(private val storageResolver: StorageResolver) {
    companion object {
        private val IS_UPGRADED_SQL = """
        SELECT * FROM ${UPGRADES.name} 
        WHERE ${UPGRADE_CLASS.name} = ?
    """.trimIndent()
        private val REGISTER_UPGRADE_SQL = """
        INSERT INTO ${UPGRADES.name} (${UPGRADE_CLASS.name}) VALUES (?) 
        ON CONFLICT DO NOTHING 
        """.trimIndent()
    }

    private val UPDATE_UPGRADE_STATUS_SQL = """
        UPDATE ${UPGRADES.name} SET ${UPGRADE_STATUS.name} = ?, ${LAST_UPDATE.name} = now()
        WHERE ${UPGRADE_CLASS.name} = ? 
    """.trimIndent()

    fun registerUpgrade(upgrade: PreHazelcastUpgradeService) {
        val upgradeClass = getUpgradeClassText(upgrade)
        storageResolver.getPlatformStorage().connection.use { c ->
            c.prepareStatement(REGISTER_UPGRADE_SQL).use { ps ->
                ps.setString(1, upgradeClass)
                ps.executeUpdate()
            }
        }
    }

    fun completeUpgrade(connection: Connection, upgrade: PreHazelcastUpgradeService) {
        updateUpgradeStatus(connection, upgrade, UpgradeStatus.Complete)
    }

    private fun updateUpgradeStatus(
        connection: Connection,
        upgrade: PreHazelcastUpgradeService,
        upgradeStatus: UpgradeStatus,
    ) {
        val upgradeClass = getUpgradeClassText(upgrade)
        connection.prepareStatement(UPDATE_UPGRADE_STATUS_SQL).use { ps ->
            ps.setString(1, upgradeStatus.name)
            ps.setString(2, upgradeClass)
        }
    }

    fun failUpgrade(upgrade: PreHazelcastUpgradeService) {
        storageResolver.getPlatformStorage().connection.use { c ->
            updateUpgradeStatus(c, upgrade, UpgradeStatus.Failed)
        }
    }

    /**
     * Checks if an upgrade has been applied.
     */
    fun isUpgradeComplete(upgrade:PreHazelcastUpgradeService): Boolean {
        val upgradeClass = getUpgradeClassText(upgrade)
        return storageResolver.getPlatformStorage().connection.use { c ->
            c.prepareStatement(IS_UPGRADED_SQL).use { ps ->
                ps.setString(1, upgradeClass)
                ps.executeQuery().use { rs ->
                    if (rs.next()) {
                        return UpgradeStatus.valueOf(rs.getString(UPGRADE_STATUS.name)) == UpgradeStatus.Complete
                    } else {
                        false
                    }
                }
            }
        }
    }

    private fun getUpgradeClassText(upgrade: PreHazelcastUpgradeService): String {
        return upgrade.javaClass.name
    }

}

enum class UpgradeStatus {
    Registered,
    Failed,
    Complete
}



