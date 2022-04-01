package com.openlattice.chronicle.services.studies

import com.openlattice.chronicle.study.StudyDuration
import com.openlattice.chronicle.study.StudyFeature
import com.openlattice.chronicle.study.StudyLimits
import com.openlattice.chronicle.study.StudyLimitsApi
import java.sql.Connection
import java.util.*

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
    fun reserveEnrollmentCapacity(connection: Connection, studyId: UUID, capacity: Int = 1)
    fun getAvailableEnrollmentCapactity(studyId: UUID): Int
    fun setEnrollmentCapacity(studyId: UUID, capacity: Int)

    fun setStudyDuration(studyId: UUID, studyDuration: StudyDuration)
    fun getStudyDuration(studyId: UUID): StudyDuration

    fun setDataRetentionPeriod(studyId: UUID, dataRetentionPeriod: StudyDuration)
    fun getDataRetentionPeriod(studyId: UUID): StudyDuration

    fun makeArchivable(studyId: UUID)
    fun isArchivable(studyId: UUID): Boolean

    fun getStudyFeatures(studyId: UUID): Set<StudyFeature>
    fun setStudyFeatureS(studyId: UUID, studyFeatures: Set<StudyFeature>)

    fun setStudyLimits(studyId: UUID, studyLimits: StudyLimits)
    fun getStudyLimits(studyId: UUID): StudyLimits

    fun getStudiesExceedingDurationLimit() : Set<UUID>
    fun getStudiesExcceedingDataRetentionPeriod() : Set<UUID>
}


