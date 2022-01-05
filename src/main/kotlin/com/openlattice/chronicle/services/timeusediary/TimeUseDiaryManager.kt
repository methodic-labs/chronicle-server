package com.openlattice.chronicle.services.timeusediary

import com.openlattice.chronicle.tud.TudDownloadDataType
import com.openlattice.chronicle.tud.TudResponse
import java.time.LocalDate
import java.time.OffsetDateTime
import java.util.Date
import java.util.UUID

/**
 * @author Andrew Carter andrew@openlattice.com
 */
interface TimeUseDiaryManager {
    fun submitTimeUseDiary(
        organizationId: UUID,
        studyId: UUID,
        participantId: String,
        responses: List<TudResponse>
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
        type: TudDownloadDataType,
        submissionsIds: Set<UUID>
    )
}