package com.openlattice.chronicle.services.studies

import com.openlattice.chronicle.participants.Participant
import com.openlattice.chronicle.sensorkit.SensorType
import com.openlattice.chronicle.study.Study
import com.openlattice.chronicle.study.StudyUpdate
import java.sql.Connection
import java.util.UUID

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
interface StudyManager {
    fun createStudy(connection: Connection, study: Study)
    fun createStudy(study: Study): UUID
    fun deleteStudies(connection: Connection, studyIds: Collection<UUID>): Int
    fun getOrgStudies(organizationId: UUID): List<Study>
    fun getOrganizationIdForLegacyStudy(studyId: UUID): UUID
    fun getStudies(studyIds: Collection<UUID>): Iterable<Study>
    fun getStudy(studyId: UUID): Study
    fun getStudyParticipants(studyId: UUID): Iterable<Participant>
    fun getStudySensors(studyId: UUID): Set<SensorType>
    fun getStudySettings(studyId: UUID): Map<String, Any>
    fun isNotificationsEnabled(studyId: UUID): Boolean
    fun refreshStudyCache(studyIds: Set<UUID>)
    fun registerParticipant(connection: Connection, studyId: UUID, participant: Participant): UUID
    fun registerParticipant(studyId: UUID, participant: Participant): UUID
    fun removeAllParticipantsFromStudies(connection: Connection, studyIds: Collection<UUID>): Int
    fun removeParticipantsFromStudy(connection: Connection, studyId: UUID, participantIds: Collection<String>): Int
    fun removeStudiesFromOrganizations(connection: Connection, studyIds: Collection<UUID>): Int
    fun updateStudy(connection: Connection, studyId: UUID, study: StudyUpdate)
}
