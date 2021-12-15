package com.openlattice.chronicle.services.enrollment

import com.google.common.base.Optional
import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.participants.Participant
import com.openlattice.chronicle.sources.Datasource
import org.apache.olingo.commons.api.edm.FullQualifiedName
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
            organizationId: UUID,
            studyId: UUID,
            participantId: String,
            datasourceId: String,
            datasource: Optional<Datasource>
    ): UUID

    fun isKnownDatasource(organizationId: UUID, studyId: UUID, participantId: String, datasourceId: String): Boolean
    fun isKnownParticipant(organizationId: UUID, studyId: UUID, participantId: String): Boolean

    fun getParticipantEntity(
            organizationId: UUID,
            studyId: UUID,
            participantEntityId: UUID
    ): Participant

    fun getParticipationStatus(organizationId: UUID, studyId: UUID, participantId: String): ParticipationStatus
    fun isNotificationsEnabled(organizationId: UUID, studyId: UUID): Boolean
    fun getStudyParticipantIds(organizationId: UUID, studyId: UUID): Set<String>
    fun getStudyParticipants(organizationId: UUID, studyId: UUID): Set<Participant>
    fun studyExists(organizationId: UUID, studyId: UUID): Boolean
    fun getOrganizationIdForStudy(studyId: UUID): UUID
    fun getOrganizationIdForLegacyStudy(studyId: UUID): UUID
}
