package com.openlattice.chronicle.services.studies

import com.openlattice.chronicle.participants.Participant
import com.openlattice.chronicle.services.enrollment.EnrollmentService
import com.openlattice.chronicle.study.Study
import com.openlattice.chronicle.study.StudySettings
import com.openlattice.chronicle.study.StudyUpdate
import java.sql.Connection
import java.util.UUID

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
interface StudyManager {
    fun createStudy(connection: Connection, study: Study)
    fun getStudy(studyId: UUID): Study
    fun getStudies(studyIds: Collection<UUID>): Iterable<Study>
    fun getOrgStudies(organizationId: UUID): List<Study>
    fun updateStudy(connection: Connection, studyId: UUID, study: StudyUpdate)
    fun registerParticipant(connection: Connection, studyId: UUID, participant: Participant): UUID
    fun isNotificationsEnabled(studyId: UUID): Boolean

    fun getOrganizationIdForLegacyStudy(studyId: UUID): UUID
    fun refreshStudyCache(studyIds: Set<UUID>)
    fun getStudySettings(studyId: UUID): Map<String, Any>
}