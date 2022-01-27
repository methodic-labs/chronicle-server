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
        studyId: UUID,
        participantId: String,
        datasourceId: String,
        datasource: Datasource
    ): UUID {
        logger.info(
            "attempting to register data source" + ChronicleServerUtil.ORG_STUDY_PARTICIPANT_DATASOURCE,
            studyId,
            participantId,
            datasourceId
        )
        val isKnownParticipant = isKnownParticipant(studyId, participantId)
        if (!isKnownParticipant) {
            logger.error(
                "unknown participant, unable to register datasource" + ChronicleServerUtil.ORG_STUDY_PARTICIPANT_DATASOURCE,
                studyId,
                participantId,
                datasourceId
            )
            throw AccessDeniedException("unknown participant, unable to register datasource")
        }


        return when (datasource) {
            is AndroidDevice -> registerAndroidDeviceOrGetId(
                studyId,
                participantId,
                datasourceId,
                datasource
            )
            else -> throw UnsupportedOperationException("${datasource.javaClass.name} is not a supported datasource.")
        }
    }

    private fun registerAndroidDeviceOrGetId(
        studyId: UUID,
        participantId: String,
        datasourceId: String,
        datasource: AndroidDevice
    ): UUID {
        TODO("Not yet implemented")
    }

    override fun isKnownDatasource(
        studyId: UUID,
        participantId: String,
        datasourceId: String
    ): Boolean {
        TODO("Not yet implemented")
    }

    override fun isKnownParticipant(studyId: UUID, participantId: String): Boolean {
        TODO("Not yet implemented")
    }

    override fun getParticipantEntity(
        studyId: UUID, participantEntityKeyId: UUID
    ): Participant {
        TODO("Not yet implemented")
    }

    override fun getParticipationStatus(
        studyId: UUID,
        participantId: String
    ): ParticipationStatus {
        val status: ParticipationStatus
        logger.info(
            "getting participation status" + ChronicleServerUtil.ORG_STUDY_PARTICIPANT, studyId,
            participantId
        )
        TODO("Not yet implemented")
    }

    override fun isNotificationsEnabled(studyId: UUID): Boolean {
        logger.info("Checking notifications enabled on studyId = {}", studyId)
        TODO("Not yet implemented")
    }

    override fun getStudyParticipantIds(studyId: UUID): Set<String> {
        TODO("Not yet implemented")
    }

    override fun getStudyParticipants(studyId: UUID): Set<Participant> {
        TODO("Not yet implemented")
    }

    override fun studyExists(studyId: UUID): Boolean {
        TODO("Not yet implemented")
    }

    override fun getOrganizationIdForStudy(studyId: UUID): UUID {
        TODO("Not yet implemented")
    }

    override fun getOrganizationIdForLegacyStudy(studyId: UUID): UUID {
        TODO("Not yet implemented")
    }
}
