package com.openlattice.chronicle.services.enrollment

import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.participants.Participant
import com.openlattice.chronicle.sources.Datasource
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
        datasource: Datasource
    ): UUID

    fun isKnownDatasource(studyId: UUID, participantId: String, datasourceId: String): Boolean
    fun isKnownParticipant(studyId: UUID, participantId: String): Boolean

    fun getParticipantEntity(

        studyId: UUID,
        participantEntityId: UUID
    ): Participant

    fun getParticipationStatus(studyId: UUID, participantId: String): ParticipationStatus
    fun isNotificationsEnabled(studyId: UUID): Boolean
    fun getStudyParticipantIds(studyId: UUID): Set<String>
    fun getStudyParticipants(studyId: UUID): Set<Participant>
    fun studyExists(studyId: UUID): Boolean
    fun getOrganizationIdForStudy(studyId: UUID): UUID
    fun getOrganizationIdForLegacyStudy(studyId: UUID): UUID
}
