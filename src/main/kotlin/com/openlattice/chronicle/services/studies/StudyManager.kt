package com.openlattice.chronicle.services.studies

import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.participants.Participant
import com.openlattice.chronicle.participants.ParticipantStats
import com.openlattice.chronicle.sensorkit.SensorType
import com.openlattice.chronicle.sources.SourceDevice
import com.openlattice.chronicle.study.Study
import com.openlattice.chronicle.study.StudySetting
import com.openlattice.chronicle.study.StudySettingType
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
    fun getParticipantStats(studyId: UUID, participantId: String): ParticipantStats?
    fun getStudies(studyIds: Collection<UUID>): Iterable<Study>
    fun getStudy(studyId: UUID): Study
    fun getStudyId(maybeLegacyMaybeRealStudyId: UUID): UUID?
    fun getStudyParticipantStats(studyId: UUID): Map<String, ParticipantStats>
    fun getStudyParticipants(studyId: UUID): Iterable<Participant>
    fun getStudySensors(studyId: UUID): Set<SensorType>
    fun getStudySettings(studyId: UUID): Map<StudySettingType, StudySetting>
    fun insertOrUpdateParticipantStats(stats: ParticipantStats)
    fun isNotificationsEnabled(studyId: UUID): Boolean
    fun isValidStudy(studyId: UUID): Boolean
    fun refreshStudyCache(studyIds: Set<UUID>)

    /**
     * Callers of this function must ensure that load the participant ace after they commit changes.
     */
    fun registerParticipant(connection: Connection, studyId: UUID, participant: Participant): UUID
    fun registerParticipant(studyId: UUID, participant: Participant): UUID
    fun removeAllParticipantsFromStudies(connection: Connection, studyIds: Collection<UUID>): Int
    fun removeParticipantsFromStudy(connection: Connection, studyId: UUID, participantIds: Collection<String>): Int
    fun removeStudiesFromOrganizations(connection: Connection, studyIds: Collection<UUID>): Int
    fun updateStudy(connection: Connection, studyId: UUID, study: StudyUpdate)
    fun getStudyPhoneNumber(studyId: UUID): String?
    fun updateParticipationStatus(studyId: UUID, participantId: String, participationStatus: ParticipationStatus)
    fun countStudyParticipants(connection: Connection, studyIds: Set<UUID>): Map<UUID, Long>
    fun countStudyParticipants(studyId: UUID): Long
    fun countStudyParticipants(studyIds: Set<UUID>): Map<UUID, Long>

    fun updateLastDevicePing( studyId: UUID,participantId: String, sourceDevice:SourceDevice)
    fun updateLastDevicePing(studyId: UUID, participantId: String)
}
