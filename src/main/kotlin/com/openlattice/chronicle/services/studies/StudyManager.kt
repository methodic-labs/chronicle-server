package com.openlattice.chronicle.services.studies

import com.openlattice.chronicle.participants.Participant
import com.openlattice.chronicle.participants.ParticipantStats
import com.openlattice.chronicle.sensorkit.SensorType
import com.openlattice.chronicle.study.Study
import com.openlattice.chronicle.study.StudyUpdate
import java.sql.Connection
import java.util.UUID

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
interface StudyManager {
    fun createStudy(study: Study): UUID
    fun createStudy(connection: Connection, study: Study)
    fun getStudy(studyId: UUID): Study
    fun getStudies(studyIds: Collection<UUID>): Iterable<Study>
    fun deleteStudies(connection: Connection, studyIds: Collection<UUID>): Int
    fun getOrgStudies(organizationId: UUID): List<Study>
    fun updateStudy(connection: Connection, studyId: UUID, study: StudyUpdate)
    fun registerParticipant(connection: Connection, studyId: UUID, participant: Participant): UUID
    fun isNotificationsEnabled(studyId: UUID): Boolean
    fun getOrganizationIdForLegacyStudy(studyId: UUID): UUID
    fun refreshStudyCache(studyIds: Set<UUID>)
    fun getStudySettings(studyId: UUID): Map<String, Any>
    fun getStudyParticipants(studyId: UUID): Iterable<Participant>
    fun getStudySensors(studyId: UUID): Set<SensorType>
    fun registerParticipant(studyId: UUID, participant: Participant): UUID
    fun getStudyParticipantStats(studyId: UUID): Map<String, ParticipantStats>
}
