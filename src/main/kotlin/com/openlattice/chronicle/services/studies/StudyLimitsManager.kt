package com.openlattice.chronicle.services.studies

import java.sql.Connection
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@getmethodic.com&gt;
 */
interface StudyLimitsManager {
    /**
     * Allocates enrollment capacity for this transaction. It used SELECT FOR UPDATE to lock rows
     * in the limits table, preventing other threads from allocating enrollment. This is necessary
     * because you two researchers adding at the same time could end up going over the limit.
     */
    fun reserveEnrollmentCapacity(connection: Connection, studyId: UUID)
    fun hasEnrollmentCapacity(studyId: UUID, participantCount: Int = 1)
    fun getAvailableEnrollmentCapactity(studyId: UUID): Int
    fun setEnrollmentCapacity(studyId: UUID, capacity:Int)

    fun getStudyDataCollectionTimeframe(studyId: UUID): String
    fun setStudyDataCollectionTimeframe(studyId: UUID, interval: String)

    fun setStudyArchiveTimeframe(studyId: UUID, interval: String)
    fun getStudyArchiveTimeframe(studyId: UUID): String

    fun makeArchivable(studyId: UUID)
    fun isArchivable(studyId: UUID): Boolean
}


data class Timeframe(
    val years: Short,
    val months: Short,
    val days: Short,
)