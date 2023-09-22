package com.openlattice.chronicle.storage.tasks

import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.geekbeast.tasks.HazelcastFixedRateTask
import com.geekbeast.tasks.Task
import com.google.common.util.concurrent.ListeningExecutorService
import com.google.common.util.concurrent.MoreExecutors
import com.openlattice.chronicle.mapstores.stats.ParticipantKey
import com.openlattice.chronicle.participants.ParticipantStats
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.services.studies.StudyManager
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.participantStatsAndroidSql
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.participantStatsIosSql
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.security.InvalidParameterException
import java.time.LocalDate
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class RecalculateParticipantStatsTask : HazelcastFixedRateTask<RecalculateParticipantStatsTaskDependencies> {
    companion object {
        private val logger = LoggerFactory.getLogger(RecalculateParticipantStatsTask::class.java)

        private val executor: ListeningExecutorService =
            MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1))
    }

    override fun getInitialDelay(): Long = 0

    override fun getPeriod(): Long = 12

    override fun getTimeUnit(): TimeUnit = TimeUnit.HOURS

    override fun runTask() {
        val f = executor.submit {
            recalculateParticipantStats()
        }
        try {
            f.get(4, TimeUnit.HOURS)
        } catch (timeoutException: TimeoutException) {
            logger.error("Timed out after one hour when moving events to event storage.", timeoutException)
            f.cancel(true)
        } catch (ex: Exception) {
            logger.error("Exception when moving events to event storage.", ex)
        }
    }

    override fun getName(): String = Task.MOVE_IOS_DATA_TO_EVENT_STORAGE.name
    override fun getDependenciesClass(): Class<out RecalculateParticipantStatsTaskDependencies> =
        RecalculateParticipantStatsTaskDependencies::class.java

    private fun recalculateParticipantStats() {
        logger.info("Starting recalculation of participant stats...")
        with(getDependency()) {
            val (_, events) = storageResolver.getDefaultEventStorage()
            val studyIds = studyService.getAllStudyIds()
            recalculateParticipantStats(studyService, studyIds, events, ParticipantStat.Ios)
            recalculateParticipantStats(studyService, studyIds, events, ParticipantStat.Android)
        }
    }

    private fun recalculateParticipantStats(
        studyService: StudyManager,
        studyIds: Iterable<UUID>,
        hds: HikariDataSource,
        statType: ParticipantStat
    ) {
        val sql = when (statType) {
            ParticipantStat.Ios -> participantStatsIosSql
            ParticipantStat.Android -> participantStatsAndroidSql
            ParticipantStat.Tud -> throw InvalidParameterException("Not yet implemented for time use diary.")
        }
        studyIds.asSequence().forEach { studyId ->
            BasePostgresIterable(
                PreparedStatementHolderSupplier(
                    hds,
                    sql,
                    fetchSize = 65536,
                ) {
                    it.setObject(1, studyId)
                }
            ) {
                val studyId = ResultSetAdapters.studyId(it)
                val participantId = it.getString(PARTICIPANT_ID.name)
                val uniqueDate = ResultSetAdapters.uniqueDates(it)
                ParticipantKey(studyId, participantId) to uniqueDate
            }.fold(mutableMapOf<ParticipantKey, MutableSet<LocalDate>>()) { uniqueDatesByStudyParticipant, p ->
                uniqueDatesByStudyParticipant.getOrPut(p.first) {
                    mutableSetOf()
                }.add(p.second)
                uniqueDatesByStudyParticipant
            }.forEach {
                studyService.insertOrUpdateParticipantStats(
                    when (statType) {
                        ParticipantStat.Ios -> ParticipantStats(
                            studyId = it.key.studyId,
                            participantId = it.key.participantId,
                            iosUniqueDates = it.value
                        )

                        ParticipantStat.Android -> ParticipantStats(
                            studyId = it.key.studyId,
                            participantId = it.key.participantId,
                            iosUniqueDates = it.value
                        )
//This one won't get used for a while.
                        ParticipantStat.Tud -> ParticipantStats(
                            studyId = it.key.studyId,
                            participantId = it.key.participantId,
                            tudUniqueDates = it.value
                        )

                    }
                )
            }

        }
    }
}

internal enum class ParticipantStat {
    Android,
    Ios,
    Tud
}