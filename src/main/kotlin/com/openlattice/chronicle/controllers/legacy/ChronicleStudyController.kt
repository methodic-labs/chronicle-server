package com.openlattice.chronicle.controllers.legacy

import com.codahale.metrics.annotation.Timed
import com.google.common.base.Optional
import com.openlattice.chronicle.ChronicleStudyApi
import com.openlattice.chronicle.data.ChronicleAppsUsageDetails
import com.openlattice.chronicle.data.LegacyChronicleQuestionnaire
import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.services.studies.StudyService
import com.openlattice.chronicle.services.surveys.SurveysManager
import com.openlattice.chronicle.sources.SourceDevice
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
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
        @RequestBody datasource: Optional<SourceDevice>
    ): UUID {
        val realStudyId = studyService.getStudyId(studyId)
        checkNotNull(realStudyId) { "invalid study id" }
        return enrollmentManager.registerDatasource(realStudyId, participantId, datasourceId, datasource.get())
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
        //  validate that this device belongs to this participant in this study
        //  look up in association entitySet between device and participant, and device and study to see if it exists
        //  DataApi.getEntity(entitySetId :UUID, entityKeyId :UUID)
        return enrollmentManager.isKnownDatasource(studyId, participantId, datasourceId)
    }

    @Timed
    @RequestMapping(
        path = [ChronicleStudyApi.PARTICIPANT_PATH + ChronicleStudyApi.DATA_PATH + ChronicleStudyApi.STUDY_ID_PATH + ChronicleStudyApi.PARTICIPANT_ID_PATH + ChronicleStudyApi.APPS],
        method = [RequestMethod.GET], produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getParticipantAppsUsageData(
        @PathVariable(ChronicleStudyApi.STUDY_ID) studyId: UUID,
        @PathVariable(ChronicleStudyApi.PARTICIPANT_ID) participantId: String,
        @RequestParam(value = ChronicleStudyApi.DATE) date: String
    ): List<ChronicleAppsUsageDetails> {
//        val organizationId = studyManager.getOrganizationIdForLegacyStudy(studyId)
//        return surveysManager.getParticipantAppsUsageData(organizationId, studyId, participantId, date)
        TODO("Not needed for v3. To remove")
    }

    @Timed
    @RequestMapping(
        path = [ChronicleStudyApi.STUDY_ID_PATH + ChronicleStudyApi.NOTIFICATIONS], method = [RequestMethod.GET]
    )
    override fun isNotificationsEnabled(
        @PathVariable(ChronicleStudyApi.STUDY_ID) studyId: UUID
    ): Boolean {
        return studyService.isNotificationsEnabled(studyId)
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
        return enrollmentManager.getParticipationStatus(studyId, participantId)
    }

    @RequestMapping(
        path = [ChronicleStudyApi.PARTICIPANT_PATH + ChronicleStudyApi.DATA_PATH + ChronicleStudyApi.STUDY_ID_PATH + ChronicleStudyApi.PARTICIPANT_ID_PATH + ChronicleStudyApi.APPS],
        method = [RequestMethod.POST], consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun submitAppUsageSurvey(
        @PathVariable(ChronicleStudyApi.STUDY_ID) studyId: UUID,
        @PathVariable(ChronicleStudyApi.PARTICIPANT_ID) participantId: String,
        @RequestBody associationDetails: Map<UUID, Map<FullQualifiedName, Set<Any>>>
    ) {
//        val organizationId = studyManager.getOrganizationIdForLegacyStudy(studyId)
//        surveysManager.submitAppUsageSurvey(organizationId, studyId, participantId, associationDetails)
        TODO("Not needed for v3. To remove")
    }

    @Timed
    @RequestMapping(
        path = [ChronicleStudyApi.STUDY_ID_PATH + ChronicleStudyApi.QUESTIONNAIRE + ChronicleStudyApi.ENTITY_KEY_ID_PATH],
        method = [RequestMethod.GET], produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getChronicleQuestionnaire(
        @PathVariable(ChronicleStudyApi.STUDY_ID) studyId: UUID,
        @PathVariable(ChronicleStudyApi.ENTITY_KEY_ID) questionnaireEKID: UUID
    ): LegacyChronicleQuestionnaire {
        val organizationId = studyService.getOrganizationIdForLegacyStudy(studyId)
        return surveysManager.getLegacyQuestionnaire(organizationId, studyId, questionnaireEKID)
    }

    @Timed
    @RequestMapping(
        path = [ChronicleStudyApi.STUDY_ID_PATH + ChronicleStudyApi.PARTICIPANT_ID_PATH + ChronicleStudyApi.QUESTIONNAIRE],
        method = [RequestMethod.POST], consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun submitQuestionnaire(
        @PathVariable(ChronicleStudyApi.STUDY_ID) studyId: UUID,
        @PathVariable(ChronicleStudyApi.PARTICIPANT_ID) participantId: String,
        @RequestBody questionnaireResponses: Map<UUID, Map<FullQualifiedName, Set<Any>>>
    ) {
        val organizationId = studyService.getOrganizationIdForLegacyStudy(studyId)
        surveysManager.submitLegacyQuestionnaire(organizationId, studyId, participantId, questionnaireResponses)
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
        return surveysManager.getLegacyStudyQuestionnaires(organizationId, studyId)
    }

    @RequestMapping(
        path = [ChronicleStudyApi.STUDY_ID_PATH + ChronicleStudyApi.PARTICIPANT_ID_PATH + ChronicleStudyApi.TIME_USE_DIARY],
        method = [RequestMethod.POST], consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun submitTimeUseDiarySurvey(
        @PathVariable(ChronicleStudyApi.STUDY_ID) studyId: UUID,
        @PathVariable(ChronicleStudyApi.PARTICIPANT_ID) participantId: String,
        @RequestBody surveyData: List<Map<FullQualifiedName, Set<Any>>>
    ) {
//        val organizationId = studyManager.getOrganizationIdForLegacyStudy(studyId)
//        surveysManager.submitTimeUseDiarySurvey(organizationId, studyId, participantId, surveyData)
        TODO("Not needed for v3. Will remove")
    }
}
