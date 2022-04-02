package com.openlattice.chronicle.services.studies

import com.openlattice.chronicle.study.StudyDuration
import com.openlattice.chronicle.study.StudyFeature
import com.openlattice.chronicle.study.StudyLimits
import java.sql.Connection
import java.util.*
import javax.naming.InsufficientResourcesException

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@getmethodic.com&gt;
 */
interface StudyLimitsManager {
    fun initializeStudyLimits(connection: Connection, studyId: UUID, studyLimits: StudyLimits = StudyLimits())


    /**
     * Allocates enrollment capacity for this transaction. It used SELECT FOR UPDATE to lock rows
     * in the limits table, preventing other threads from allocating enrollment. This is necessary
     * because you two researchers adding at the same time could end up going over the limit.
     */
    fun lockStudyForEnrollments(connection: Connection, studyId: UUID)
    fun getEnrollmentCapacity(studyId: UUID): Int
    fun setEnrollmentCapacity(studyId: UUID, capacity: Int)

    fun setStudyDuration(studyId: UUID, studyDuration: StudyDuration)
    fun getStudyDuration(studyId: UUID): StudyDuration

    fun setDataRetentionPeriod(studyId: UUID, dataRetentionPeriod: StudyDuration)
    fun getDataRetentionPeriod(studyId: UUID): StudyDuration

    fun getStudyFeatures(studyId: UUID): Set<StudyFeature>
    fun setStudyFeatures(studyId: UUID, studyFeatures: Set<StudyFeature>)

    fun setStudyLimits(studyId: UUID, studyLimits: StudyLimits)
    fun getStudyLimits(studyId: UUID): StudyLimits

    fun getStudiesExceedingDurationLimit(): Set<UUID>
    fun getStudiesExcceedingDataRetentionPeriod(): Set<UUID>

    fun countStudyParticipants(connection: Connection, studyIds: Set<UUID>): Map<UUID, Long>
    fun countStudyParticipants(studyId: UUID): Long
    fun countStudyParticipants(studyIds: Set<UUID>): Map<UUID, Long>
    fun reserveEnrollmentCapacity(connection: Connection, studyId: UUID, capacity: Int = 1) {
        lockStudyForEnrollments(connection, studyId)
        val maxParticipantCount = getEnrollmentCapacity(studyId)
        val neededParticipants = countStudyParticipants(connection, setOf(studyId)).getValue(studyId) + capacity
        if (neededParticipants > maxParticipantCount) {
            throw InsufficientResourcesException("Insufficient remaining capacity to add particpants")
        }
    }
}


