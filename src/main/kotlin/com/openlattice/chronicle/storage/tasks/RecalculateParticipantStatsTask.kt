package com.openlattice.chronicle.storage.tasks

import com.geekbeast.configuration.postgres.PostgresFlavor
import com.geekbeast.tasks.HazelcastFixedRateTask
import com.geekbeast.tasks.Task
import com.openlattice.chronicle.storage.PostgresDataTables
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

/**
 * Monthly true-up of `participant_stats.{ios,android}_unique_dates`.
 *
 * Incremental updates happen at ingestion time (`AppDataUploadService.updateParticipantStats` for
 * Android, `MoveToIosEventStorageTask.updateParticipantStats` for iOS) and flow through the
 * `participantStats` IMap with a 5-second write-behind. This task exists to recover any dates that
 * the incremental path missed (crashes, races, late uploads inside the lookback window).
 *
 * The previous 12-hour, per-study, full-history streaming scan was the dominant Aurora read load.
 * This replacement is a single bounded SQL upsert per platform with `ON CONFLICT` set-union, so
 * older dates and concurrently-merged dates are preserved.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class RecalculateParticipantStatsTask : HazelcastFixedRateTask<RecalculateParticipantStatsTaskDependencies> {
    companion object {
        private val logger = LoggerFactory.getLogger(RecalculateParticipantStatsTask::class.java)
    }

    override fun getInitialDelay(): Long = 1
    override fun getPeriod(): Long = 30
    override fun getTimeUnit(): TimeUnit = TimeUnit.DAYS
    override fun getName(): String = Task.RECALCULATE_PARTICIPANT_STATS.name
    override fun getDependenciesClass(): Class<out RecalculateParticipantStatsTaskDependencies> =
        RecalculateParticipantStatsTaskDependencies::class.java

    override fun runTask() {
        try {
            with(getDependency()) {
                val (flavor, _) = storageResolver.getDefaultEventStorage()
                if (flavor == PostgresFlavor.REDSHIFT) {
                    logger.warn("Skipping participant stats reconciliation: only supported on Postgres-flavor event storage.")
                    return
                }

                logger.info(
                    "Starting {}-day participant stats reconciliation...",
                    PostgresDataTables.PARTICIPANT_STATS_RECONCILE_LOOKBACK_DAYS
                )

                storageResolver.getPlatformStorage().connection.use { connection ->
                    connection.createStatement().use { stmt ->
                        val ios = stmt.executeUpdate(PostgresDataTables.reconcileIosParticipantStatsSql)
                        logger.info("Reconciled iOS unique dates for {} (study, participant) rows.", ios)
                        val android = stmt.executeUpdate(PostgresDataTables.reconcileAndroidParticipantStatsSql)
                        logger.info("Reconciled Android unique dates for {} (study, participant) rows.", android)
                    }
                }

                studyService.evictParticipantStatsCache()
                logger.info("Completed participant stats reconciliation.")
            }
        } catch (ex: Exception) {
            logger.error("Exception during participant stats reconciliation.", ex)
        }
    }
}
