package com.openlattice.chronicle.controllers.legacy

import com.codahale.metrics.annotation.Timed
import com.google.common.base.Optional
import com.openlattice.chronicle.ChronicleStudyApi
import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.participants.ParticipantStats
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.services.studies.StudyService
import com.openlattice.chronicle.services.surveys.SurveysManager
import com.openlattice.chronicle.sources.AndroidDevice
import com.openlattice.chronicle.sources.IOSDevice
import com.openlattice.chronicle.sources.SourceDevice
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RestController
import java.time.OffsetDateTime
import java.util.UUID
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@RestController
@RequestMapping(ChronicleStudyApi.CONTROLLER)
class ChronicleStudyController : ChronicleStudyApi {
    @Inject
    private lateinit var surveysManager: SurveysManager

    @Inject
    private lateinit var enrollmentManager: EnrollmentManager

    @Inject
    private lateinit var studyService: StudyService

    @Timed
    @RequestMapping(
        path = [ChronicleStudyApi.STUDY_ID_PATH + ChronicleStudyApi.PARTICIPANT_ID_PATH + ChronicleStudyApi.DATASOURCE_ID_PATH],
        method = [RequestMethod.POST], consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun enrollSource(
        @PathVariable(ChronicleStudyApi.STUDY_ID) studyId: UUID,
        @PathVariable(ChronicleStudyApi.PARTICIPANT_ID) participantId: String,
        @PathVariable(ChronicleStudyApi.DATASOURCE_ID) datasourceId: String,
        @RequestBody sourceDevice: Optional<SourceDevice>
    ): UUID {
        val realStudyId = studyService.getStudyId(studyId)
        checkNotNull(realStudyId) { "invalid study id" }
        val id = enrollmentManager.registerDatasource(realStudyId, participantId, datasourceId, sourceDevice.get())
        studyService.updateLastDevicePing(realStudyId, participantId, sourceDevice.get())
        return id
    }

    @Timed
    @RequestMapping(
        path = [ChronicleStudyApi.STUDY_ID_PATH + ChronicleStudyApi.PARTICIPANT_ID_PATH + ChronicleStudyApi.DATASOURCE_ID_PATH],
        method = [RequestMethod.GET], produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun isKnownDatasource(
        @PathVariable(ChronicleStudyApi.STUDY_ID) studyId: UUID,
        @PathVariable(ChronicleStudyApi.PARTICIPANT_ID) participantId: String,
        @PathVariable(ChronicleStudyApi.DATASOURCE_ID) datasourceId: String
    ): Boolean {
        val realStudyId = studyService.getStudyId(studyId)
        checkNotNull(realStudyId) { "invalid study id" }
        return enrollmentManager.isKnownDatasource(realStudyId, participantId, datasourceId)
    }

    @Timed
    @RequestMapping(
        path = [ChronicleStudyApi.STUDY_ID_PATH + ChronicleStudyApi.NOTIFICATIONS], method = [RequestMethod.GET]
    )
    override fun isNotificationsEnabled(
        @PathVariable(ChronicleStudyApi.STUDY_ID) studyId: UUID
    ): Boolean {
        val realStudyId = studyService.getStudyId(studyId)
        checkNotNull(realStudyId) { "invalid study id" }
        return studyService.isNotificationsEnabled(realStudyId)
    }

    @Timed
    @RequestMapping(
        path = [ChronicleStudyApi.STUDY_ID_PATH + ChronicleStudyApi.PARTICIPANT_ID_PATH + ChronicleStudyApi.ENROLLMENT_STATUS],
        method = [RequestMethod.GET]
    )
    override fun getParticipationStatus(
        @PathVariable(ChronicleStudyApi.STUDY_ID) studyId: UUID,
        @PathVariable(ChronicleStudyApi.PARTICIPANT_ID) participantId: String
    ): ParticipationStatus {
        val realStudyId = studyService.getStudyId(studyId)
        checkNotNull(realStudyId) { "invalid study id" }
        return enrollmentManager.getParticipationStatus(realStudyId, participantId)
    }

    @Timed
    @RequestMapping(
        path = [ChronicleStudyApi.STUDY_ID_PATH + ChronicleStudyApi.QUESTIONNAIRES], method = [RequestMethod.GET],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getStudyQuestionnaires(
        @PathVariable(ChronicleStudyApi.STUDY_ID) studyId: UUID
    ): Map<UUID, Map<FullQualifiedName, Set<Any>>> {
        val organizationId = studyService.getOrganizationIdForLegacyStudy(studyId)
        val realStudyId = studyService.getStudyId(studyId)
        checkNotNull(realStudyId) { "invalid study id" }
        return surveysManager.getLegacyStudyQuestionnaires(organizationId, realStudyId)
    }
}
