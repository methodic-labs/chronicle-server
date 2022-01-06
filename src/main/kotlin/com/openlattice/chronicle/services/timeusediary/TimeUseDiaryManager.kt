package com.openlattice.chronicle.services.timeusediary

import com.openlattice.chronicle.tud.TimeUseDiaryDownloadDataType
import com.openlattice.chronicle.tud.TimeUseDiaryResponse
import java.time.LocalDate
import java.time.OffsetDateTime
import java.util.UUID

/**
 * @author Andrew Carter andrew@openlattice.com
 */
interface TimeUseDiaryManager {
    fun submitTimeUseDiary(
        organizationId: UUID,
        studyId: UUID,
        participantId: String,
        responses: List<TimeUseDiaryResponse>
    ): UUID

    fun getSubmissionByDate(
        organizationId: UUID,
        studyId: UUID,
        participantId: String,
        startDate: OffsetDateTime,
        endDate: OffsetDateTime
    ): Map<LocalDate, Set<UUID>>

    fun downloadTimeUseDiaryData(
        organizationId: UUID,
        studyId: UUID,
        participantId: String,
        type: TimeUseDiaryDownloadDataType,
        submissionsIds: Set<UUID>
    )
}