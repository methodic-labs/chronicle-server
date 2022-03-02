package com.openlattice.chronicle.services.timeusediary

import com.openlattice.chronicle.converters.PostgresDownloadWrapper
import com.openlattice.chronicle.timeusediary.TimeUseDiaryDownloadDataType
import com.openlattice.chronicle.timeusediary.TimeUseDiaryResponse
import java.sql.Connection
import java.time.LocalDate
import java.time.OffsetDateTime
import java.util.UUID

/**
 * @author Andrew Carter andrew@openlattice.com
 */
interface TimeUseDiaryManager {

    fun submitTimeUseDiary(
        connection: Connection,
        organizationId: UUID,
        studyId: UUID,
        participantId: String,
        responses: List<TimeUseDiaryResponse>
    ): UUID

    fun getParticipantTUDSubmissionsByDate(
        organizationId: UUID,
        studyId: UUID,
        participantId: String,
        startDate: OffsetDateTime,
        endDate: OffsetDateTime,
    ): Map<OffsetDateTime, Set<UUID>>

    fun getStudyTUDSubmissionsByDate(
        studyId: UUID,
        startDate: OffsetDateTime,
        endDate: OffsetDateTime,
    ): Map<OffsetDateTime, Set<UUID>>

    fun downloadTimeUseDiaryData(
        organizationId: UUID,
        studyId: UUID,
        participantId: String,
        downloadType: TimeUseDiaryDownloadDataType,
        submissionIds: Set<UUID>
    ): PostgresDownloadWrapper
}
