package com.openlattice.chronicle.controllers.legacy

import com.codahale.metrics.annotation.Timed
import com.google.common.base.Optional
import com.openlattice.chronicle.ChronicleStudyApi
import com.openlattice.chronicle.data.ChronicleAppsUsageDetails
import com.openlattice.chronicle.data.ChronicleQuestionnaire
import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.services.surveys.SurveysManager
import com.openlattice.chronicle.sources.Datasource
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.util.*
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

    @Timed
    @RequestMapping(
            path = [ChronicleStudyApi.STUDY_ID_PATH + ChronicleStudyApi.PARTICIPANT_ID_PATH + ChronicleStudyApi.DATASOURCE_ID_PATH],
            method = [RequestMethod.POST], consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun enrollSource(
            @PathVariable(ChronicleStudyApi.STUDY_ID) studyId: UUID,
            @PathVariable(ChronicleStudyApi.PARTICIPANT_ID) participantId: String,
            @PathVariable(ChronicleStudyApi.DATASOURCE_ID) datasourceId: String,
            @RequestBody datasource: Optional<Datasource>
    ): UUID {
        val organizationId = enrollmentManager.getOrganizationIdForLegacyStudy( studyId )
        return enrollmentManager.registerDatasource(organizationId, studyId, participantId, datasourceId, datasource)
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
        val organizationId = enrollmentManager.getOrganizationIdForLegacyStudy( studyId )
        //  validate that this device belongs to this participant in this study
        //  look up in association entitySet between device and participant, and device and study to see if it exists
        //  DataApi.getEntity(entitySetId :UUID, entityKeyId :UUID)
        return enrollmentManager.isKnownDatasource(organizationId, studyId, participantId, datasourceId)
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
//        val organizationId = enrollmentManager.getOrganizationIdForLegacyStudy( studyId )
//        return surveysManager.getParticipantAppsUsageData(organizationId, studyId, participantId, date)
        TODO("This shouldn't have to exist. Will remove")
    }

    @Timed
    @RequestMapping(
            path = [ChronicleStudyApi.STUDY_ID_PATH + ChronicleStudyApi.NOTIFICATIONS], method = [RequestMethod.GET]
    )
    override fun isNotificationsEnabled(
            @PathVariable(ChronicleStudyApi.STUDY_ID) studyId: UUID
    ): Boolean {
        val organizationId = enrollmentManager.getOrganizationIdForLegacyStudy( studyId )
        return enrollmentManager.isNotificationsEnabled(organizationId, studyId)
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
        val organizationId = enrollmentManager.getOrganizationIdForLegacyStudy( studyId )
        return enrollmentManager.getParticipationStatus(organizationId, studyId, participantId)
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
//        val organizationId = enrollmentManager.getOrganizationIdForLegacyStudy( studyId )
//        surveysManager.submitAppUsageSurvey(organizationId, studyId, participantId, associationDetails)
        TODO("Remove this during clean up")
    }

    @Timed
    @RequestMapping(
            path = [ChronicleStudyApi.STUDY_ID_PATH + ChronicleStudyApi.QUESTIONNAIRE + ChronicleStudyApi.ENTITY_KEY_ID_PATH],
            method = [RequestMethod.GET], produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getChronicleQuestionnaire(
            @PathVariable(ChronicleStudyApi.STUDY_ID) studyId: UUID,
            @PathVariable(ChronicleStudyApi.ENTITY_KEY_ID) questionnaireEKID: UUID
    ): ChronicleQuestionnaire {
        val organizationId = enrollmentManager.getOrganizationIdForLegacyStudy( studyId )
        return surveysManager.getQuestionnaire(organizationId, studyId, questionnaireEKID)
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
        val organizationId = enrollmentManager.getOrganizationIdForLegacyStudy( studyId )
        surveysManager.submitQuestionnaire(organizationId, studyId, participantId, questionnaireResponses)
    }

    @Timed
    @RequestMapping(
            path = [ChronicleStudyApi.STUDY_ID_PATH + ChronicleStudyApi.QUESTIONNAIRES], method = [RequestMethod.GET],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getStudyQuestionnaires(
            @PathVariable(ChronicleStudyApi.STUDY_ID) studyId: UUID
    ): Map<UUID, Map<FullQualifiedName, Set<Any>>> {
        val organizationId = enrollmentManager.getOrganizationIdForLegacyStudy( studyId )
        return surveysManager.getStudyQuestionnaires(organizationId, studyId)
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
//        val organizationId = enrollmentManager.getOrganizationIdForLegacyStudy( studyId )
//        surveysManager.submitTimeUseDiarySurvey(organizationId, studyId, participantId, surveyData)
        TODO("To remove this endpoint later during v2 cleanup. TimeUseDiary stuff will eventually be in its own controller")
    }
}
