package com.openlattice.chronicle.services.enrollment

import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.participants.Participant
import com.openlattice.chronicle.sources.SourceDevice
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
interface EnrollmentManager {
    fun registerDatasource(

        studyId: UUID,
        participantId: String,
        datasourceId: String,
        sourceDevice: SourceDevice
    ): UUID

    fun isKnownDatasource(studyId: UUID, participantId: String, sourceDeviceId: String): Boolean
    fun isKnownParticipant(studyId: UUID, participantId: String): Boolean

    fun getParticipant(studyId: UUID, participantId: String): Participant
    fun getParticipationStatus(studyId: UUID, participantId: String): ParticipationStatus
    fun isNotificationsEnabled(studyId: UUID): Boolean
    fun getStudyParticipantIds(studyId: UUID): Set<String>
    fun getStudyParticipants(studyId: UUID): Set<Participant>
    fun studyExists(studyId: UUID): Boolean
    fun getOrganizationIdForStudy(studyId: UUID): UUID
    fun getOrganizationIdForLegacyStudy(studyId: UUID): UUID
    fun getDeviceId(studyId: UUID, participantId: String, sourceDeviceId: String): UUID
}
