package com.openlattice.chronicle.studies.tasks

import com.geekbeast.tasks.HazelcastFixedRateTask
import com.geekbeast.tasks.HazelcastTaskDependencies
import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.services.studies.StudyLimitsManager
import com.openlattice.chronicle.services.studies.StudyManager
import com.openlattice.chronicle.storage.StorageResolver
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class StudyLimitsEnforcementTask : HazelcastFixedRateTask<StudyLimitsEnforcementTaskDependencies> {
    companion object {
        private val logger = LoggerFactory.getLogger(StudyLimitsEnforcementTask::class.java)
    }

    override fun getInitialDelay(): Long = 10_000

    override fun getPeriod(): Long = 1

    override fun getTimeUnit(): TimeUnit = TimeUnit.HOURS

    override fun runTask() {
        pauseParticipantsForStudiesOverDuration()
        deleteStudiesWhoseDataIsOutsideOfRetentionPeriod()
    }

    override fun getName(): String = "STUDY_LIMITS_ENFORCEMENT"

    override fun getDependenciesClass(): Class<StudyLimitsEnforcementTaskDependencies> =
        StudyLimitsEnforcementTaskDependencies::class.java

    private fun pauseParticipantsForStudiesOverDuration() {
        val deps = getDependency()
        deps.studyLimitsManager.getStudiesExceedingDurationLimit().forEach { studyId ->
            logger.info("Pausing data collection for all participants in study {}", studyId)
            deps.studyService.getStudyParticipants(studyId).forEach { participant ->
                deps.studyService.updateParticipationStatus(
                    studyId,
                    participant.participantId,
                    ParticipationStatus.PAUSED
                )
            }
        }
    }

    private fun deleteStudiesWhoseDataIsOutsideOfRetentionPeriod() {
        val deps = getDependency()
        deps.storageResolver.getPlatformStorage().connection.use { connection ->
            val studiesToDelete = deps.studyLimitsManager.getStudiesExcceedingDataRetentionPeriod()
            logger.info(
                "Deleting the following studies as they are outside of the retention period: {}",
                studiesToDelete
            )
            deps.studyService.expireStudies(
                studiesToDelete
            )
        }
    }
}

data class StudyLimitsEnforcementTaskDependencies(
    val storageResolver: StorageResolver,
    val studyLimitsManager: StudyLimitsManager,
    val studyService: StudyManager
) : HazelcastTaskDependencies