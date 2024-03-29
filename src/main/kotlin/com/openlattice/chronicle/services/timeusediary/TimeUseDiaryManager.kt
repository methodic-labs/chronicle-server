package com.openlattice.chronicle.services.timeusediary

import com.openlattice.chronicle.timeusediary.TimeUseDiaryDownloadDataType
import com.openlattice.chronicle.timeusediary.TimeUseDiaryResponse
import java.sql.Connection
import java.time.LocalDate
import java.time.OffsetDateTime
import java.util.*

/**
 * @author Andrew Carter andrew@openlattice.com
 */
interface TimeUseDiaryManager {

    fun submitTimeUseDiary(
        connection: Connection,
        studyId: UUID,
        participantId: String,
        responses: List<TimeUseDiaryResponse>
    ): UUID

    fun getParticipantTUDSubmissionsByDate(
        studyId: UUID,
        participantId: String,
        startDate: OffsetDateTime,
        endDate: OffsetDateTime,
    ): Map<OffsetDateTime, Set<UUID>>

    fun getStudyTUDSubmissionIdsByDate(
        studyId: UUID,
        startDate: OffsetDateTime,
        endDate: OffsetDateTime,
    ): Map<LocalDate, Set<UUID>>

    fun getStudyTUDSubmissions(
        studyId: UUID,
        participantIds: Set<String>? = null,
        downloadType: TimeUseDiaryDownloadDataType,
        startDate: OffsetDateTime,
        endDate: OffsetDateTime
    ): Iterable<List<Map<String, Any>>>
}
