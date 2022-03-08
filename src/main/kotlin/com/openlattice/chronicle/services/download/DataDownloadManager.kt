package com.openlattice.chronicle.services.download

import com.openlattice.chronicle.constants.ParticipantDataType
import com.openlattice.chronicle.sensorkit.SensorType
import java.time.OffsetDateTime
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface DataDownloadManager {

    fun getParticipantData(
        studyId: UUID,
        participantId: String,
        dataType: ParticipantDataType,
        token: String
    ): Iterable<Map<String, Any>>

    fun getParticipantsSensorData(
        studyId: UUID,
        participantIds: Set<String>,
        sensors: Set<SensorType>,
        startDateTime: OffsetDateTime,
        endDateTime: OffsetDateTime
    ): Iterable<Map<String, Any>>

    fun getParticipantsAppUsageSurveyData(
        studyId: UUID,
        participantIds: Set<String>,
        startDateTime: OffsetDateTime,
        endDateTime: OffsetDateTime
    ): Iterable<Map<String, Any>>

    fun getParticipantsUsageEventsData(
        studyId: UUID,
        participantIds: Set<String>,
        startDateTime: OffsetDateTime,
        endDateTime: OffsetDateTime
    ): Iterable<Map<String, Any>>

    fun getQuestionnaireResponses(
        studyId: UUID,
        questionnaireId: UUID
    ): Iterable<Map<String, Any>>
}