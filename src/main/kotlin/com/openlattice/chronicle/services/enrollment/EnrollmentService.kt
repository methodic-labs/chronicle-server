package com.openlattice.chronicle.services.enrollment

import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.participants.Participant
import com.openlattice.chronicle.services.ScheduledTasksManager
import com.openlattice.chronicle.sources.AndroidDevice
import com.openlattice.chronicle.sources.Datasource
import com.openlattice.chronicle.util.ChronicleServerUtil
import org.slf4j.LoggerFactory
import org.springframework.security.access.AccessDeniedException
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class EnrollmentService(
        private val scheduledTasksManager: ScheduledTasksManager
) : EnrollmentManager {

    companion object {
        private val logger = LoggerFactory.getLogger(EnrollmentService::class.java)
    }

    override fun registerDatasource(
            organizationId: UUID,
            studyId: UUID,
            participantId: String,
            datasourceId: String,
            datasource: Datasource
    ): UUID {
        logger.info(
                "attempting to register data source" + ChronicleServerUtil.ORG_STUDY_PARTICIPANT_DATASOURCE,
                organizationId,
                studyId,
                participantId,
                datasourceId
        )
        val isKnownParticipant = isKnownParticipant(organizationId, studyId, participantId)
        if (!isKnownParticipant) {
            logger.error(
                    "unknown participant, unable to register datasource" + ChronicleServerUtil.ORG_STUDY_PARTICIPANT_DATASOURCE,
                    organizationId,
                    studyId,
                    participantId,
                    datasourceId
            )
            throw AccessDeniedException("unknown participant, unable to register datasource")
        }

        return when( datasource ) {
            is AndroidDevice -> registerAndroidDeviceOrGetId( organizationId, studyId, participantId, datasourceId, datasource )
            else -> throw UnsupportedOperationException("${datasource.javaClass.name} is not a supported datasource.")
        }
    }

    private fun registerAndroidDeviceOrGetId(
            organizationId: UUID,
            studyId: UUID,
            participantId: String,
            datasourceId: String,
            datasource: AndroidDevice
    ): UUID {
        TODO("Not yet implemented")
    }

    override fun isKnownDatasource(
            organizationId: UUID,
            studyId: UUID,
            participantId: String,
            datasourceId: String
    ): Boolean {
        TODO("Not yet implemented")
    }

    override fun isKnownParticipant(organizationId: UUID, studyId: UUID, participantId: String): Boolean {
        TODO("Not yet implemented")
    }

    override fun getParticipantEntity(
            organizationId: UUID, studyId: UUID, participantEntityKeyId: UUID
    ): Participant {
        TODO("Not yet implemented")
    }

    override fun getParticipationStatus(
            organizationId: UUID,
            studyId: UUID,
            participantId: String
    ): ParticipationStatus {
        val status: ParticipationStatus
        logger.info(
                "getting participation status" + ChronicleServerUtil.ORG_STUDY_PARTICIPANT, organizationId, studyId,
                participantId
        )
        TODO("Not yet implemented")
    }

    override fun isNotificationsEnabled(organizationId: UUID, studyId: UUID): Boolean {
        logger.info("Checking notifications enabled on studyId = {}, organization = {}", studyId, organizationId)
        TODO("Not yet implemented")
    }

    override fun getStudyParticipantIds(organizationId: UUID, studyId: UUID): Set<String> {
        TODO("Not yet implemented")
    }

    override fun getStudyParticipants(organizationId: UUID, studyId: UUID): Set<Participant> {
       TODO("Not yet implemented")
    }

    override fun studyExists(organizationId: UUID, studyId: UUID): Boolean {
        TODO("Not yet implemented")
    }

    override fun getOrganizationIdForStudy(studyId: UUID): UUID {
        TODO("Not yet implemented")
    }

    override fun getOrganizationIdForLegacyStudy(studyId: UUID): UUID {
        TODO("Not yet implemented")
    }
}
