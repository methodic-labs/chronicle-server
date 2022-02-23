package com.openlattice.chronicle.controllers.v2

import com.codahale.metrics.annotation.Timed
import com.google.common.base.Optional
import com.google.common.collect.SetMultimap
import com.openlattice.chronicle.api.ChronicleApi
import com.openlattice.chronicle.data.ChronicleAppsUsageDetails
import com.openlattice.chronicle.data.LegacyChronicleQuestionnaire
import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.services.legacy.LegacyEdmResolver
import com.openlattice.chronicle.services.legacy.LegacyUtil
import com.openlattice.chronicle.services.settings.OrganizationSettingsManager
import com.openlattice.chronicle.services.studies.StudyManager
import com.openlattice.chronicle.services.surveys.SurveysManager
import com.openlattice.chronicle.services.upload.AppDataUploadManager
import com.openlattice.chronicle.settings.AppComponent
import com.openlattice.chronicle.sources.SourceDevice
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.security.InvalidParameterException
import java.util.*
import javax.inject.Inject

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@RestController
@RequestMapping(ChronicleApi.CONTROLLER)
class ChronicleControllerV2 : ChronicleApi {
    @Inject
    private lateinit var dataUploadManager: AppDataUploadManager

    @Inject
    private lateinit var enrollmentManager: EnrollmentManager

    @Inject
    private lateinit var surveysManager: SurveysManager

    @Inject
    private lateinit var organizationSettingsManager: OrganizationSettingsManager

    @Inject
    private lateinit var studyManager: StudyManager

    @Timed
    @RequestMapping(
            path = [ChronicleApi.ORGANIZATION_ID_PATH + ChronicleApi.STUDY_ID_PATH + ChronicleApi.PARTICIPANT_ID_PATH + ChronicleApi.DATASOURCE_ID_PATH + ChronicleApi.ENROLL_PATH],
            method = [RequestMethod.POST], consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun enroll(
            @PathVariable(ChronicleApi.ORGANIZATION_ID) organizationId: UUID,
            @PathVariable(ChronicleApi.STUDY_ID) studyId: UUID,
            @PathVariable(ChronicleApi.PARTICIPANT_ID) participantId: String,
            @PathVariable(ChronicleApi.DATASOURCE_ID) datasourceId: String,
            @RequestBody datasource: Optional<SourceDevice>
    ): UUID {
        if (datasource.isPresent) {
            return enrollmentManager.registerDatasource(
                    studyId,
                    participantId,
                    datasourceId,
                    datasource.get()
            )
        } else {
            throw InvalidParameterException("Datasource must be specified when enrolling.")
        }
    }

    @Timed
    @RequestMapping(
            path = [ChronicleApi.ORGANIZATION_ID_PATH + ChronicleApi.STUDY_ID_PATH + ChronicleApi.NOTIFICATIONS_PATH],
            method = [RequestMethod.GET]
    )
    override fun isNotificationsEnabled(
            @PathVariable(ChronicleApi.ORGANIZATION_ID) organizationId: UUID,
            @PathVariable(ChronicleApi.STUDY_ID) studyId: UUID
    ): Boolean {
        return studyManager.isNotificationsEnabled(studyId)
    }

    @Timed
    @RequestMapping(
            path = [ChronicleApi.ORGANIZATION_ID_PATH + ChronicleApi.STUDY_ID_PATH + ChronicleApi.PARTICIPANT_ID_PATH + ChronicleApi.ENROLLMENT_STATUS_PATH],
            method = [RequestMethod.GET]
    )
    override fun getParticipationStatus(
            @PathVariable(ChronicleApi.ORGANIZATION_ID) organizationId: UUID,
            @PathVariable(ChronicleApi.STUDY_ID) studyId: UUID,
            @PathVariable(ChronicleApi.PARTICIPANT_ID) participantId: String
    ): ParticipationStatus {
        return enrollmentManager.getParticipationStatus(studyId, participantId)
    }

    @Timed
    @RequestMapping(
            path = [ChronicleApi.ORGANIZATION_ID_PATH + ChronicleApi.STUDY_ID_PATH + ChronicleApi.QUESTIONNAIRE_PATH + ChronicleApi.ENTITY_KEY_ID_PATH],
            method = [RequestMethod.GET], produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getChronicleQuestionnaire(
            @PathVariable(ChronicleApi.ORGANIZATION_ID) organizationId: UUID,
            @PathVariable(ChronicleApi.STUDY_ID) studyId: UUID,
            @PathVariable(ChronicleApi.ENTITY_KEY_ID) questionnaireEKID: UUID
    ): LegacyChronicleQuestionnaire {
        return surveysManager.getLegacyQuestionnaire(organizationId, studyId, questionnaireEKID)
    }

    @Timed
    @RequestMapping(
            path = [ChronicleApi.ORGANIZATION_ID_PATH + ChronicleApi.STUDY_ID_PATH + ChronicleApi.PARTICIPANT_ID_PATH + ChronicleApi.APPS_PATH],
            method = [RequestMethod.POST], consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun submitAppUsageSurvey(
            @PathVariable(ChronicleApi.ORGANIZATION_ID) organizationId: UUID,
            @PathVariable(ChronicleApi.STUDY_ID) studyId: UUID,
            @PathVariable(ChronicleApi.PARTICIPANT_ID) participantId: String,
            @RequestBody associationDetails: Map<UUID, Map<FullQualifiedName, Set<Any>>>
    ) {
       TODO("to be removed")
    }

    @Timed
    @RequestMapping(
            path = [ChronicleApi.ORGANIZATION_ID_PATH + ChronicleApi.STUDY_ID_PATH + ChronicleApi.PARTICIPANT_ID_PATH + ChronicleApi.QUESTIONNAIRE_PATH],
            method = [RequestMethod.POST], consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun submitQuestionnaire(
            @PathVariable(ChronicleApi.ORGANIZATION_ID) organizationId: UUID,
            @PathVariable(ChronicleApi.STUDY_ID) studyId: UUID,
            @PathVariable(ChronicleApi.PARTICIPANT_ID) participantId: String,
            @RequestBody questionnaireResponses: Map<UUID, Map<FullQualifiedName, Set<Any>>>
    ) {
        surveysManager.submitLegacyQuestionnaire(organizationId, studyId, participantId, questionnaireResponses)
    }

    @Timed
    @RequestMapping(
            path = [ChronicleApi.ORGANIZATION_ID_PATH + ChronicleApi.STUDY_ID_PATH + ChronicleApi.PARTICIPANT_ID_PATH + ChronicleApi.APPS_PATH],
            method = [RequestMethod.GET], produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getParticipantAppsUsageData(
            @PathVariable(ChronicleApi.ORGANIZATION_ID) organizationId: UUID,
            @PathVariable(ChronicleApi.STUDY_ID) studyId: UUID,
            @PathVariable(ChronicleApi.PARTICIPANT_ID) participantId: String,
            @RequestParam(value = ChronicleApi.DATE) date: String
    ): List<ChronicleAppsUsageDetails> {
//        return surveysManager!!.getParticipantAppsUsageData(organizationId, studyId, participantId, date)
        TODO("won't be needed any more. to remove")
    }

    @Timed
    @RequestMapping(
            path = [ChronicleApi.ORGANIZATION_ID_PATH + ChronicleApi.STUDY_ID_PATH + ChronicleApi.QUESTIONNAIRES_PATH],
            method = [RequestMethod.GET], produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getStudyQuestionnaires(
            @PathVariable(ChronicleApi.ORGANIZATION_ID) organizationId: UUID,
            @PathVariable(ChronicleApi.STUDY_ID) studyId: UUID
    ): Map<UUID, Map<FullQualifiedName, Set<Any>>> {
        return surveysManager.getLegacyStudyQuestionnaires(organizationId, studyId)
    }

    @RequestMapping(
            path = [ChronicleApi.ORGANIZATION_ID_PATH + ChronicleApi.STUDY_ID_PATH + ChronicleApi.PARTICIPANT_ID_PATH + ChronicleApi.TIME_USE_DIARY],
            method = [RequestMethod.POST], consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun submitTimeUseDiarySurvey(
            @PathVariable(ChronicleApi.ORGANIZATION_ID) organizationId: UUID,
            @PathVariable(ChronicleApi.STUDY_ID) studyId: UUID,
            @PathVariable(ChronicleApi.PARTICIPANT_ID) participantId: String,
            @RequestBody surveyData: List<Map<FullQualifiedName, Set<Any>>>
    ) {
        // surveysManager.submitTimeUseDiarySurvey(organizationId, studyId, participantId, surveyData)
        TODO("Not needed here. To remove")
    }

    @Timed
    @RequestMapping(
            path = [ChronicleApi.ORGANIZATION_ID_PATH + ChronicleApi.STUDY_ID_PATH + ChronicleApi.PARTICIPANT_ID_PATH + ChronicleApi.DATASOURCE_ID_PATH + ChronicleApi.UPLOAD_PATH],
            method = [RequestMethod.POST], consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun upload(
            @PathVariable(ChronicleApi.ORGANIZATION_ID) organizationId: UUID,
            @PathVariable(ChronicleApi.STUDY_ID) studyId: UUID,
            @PathVariable(ChronicleApi.PARTICIPANT_ID) participantId: String,
            @PathVariable(ChronicleApi.DATASOURCE_ID) datasourceId: String,
            @RequestBody data: List<SetMultimap<UUID, Any>>
    ): Int {
        return dataUploadManager.upload(studyId, participantId, datasourceId, data)
    }

    @Timed
    @RequestMapping(
            path = [ChronicleApi.EDM_PATH], method = [RequestMethod.POST],
            consumes = [MediaType.APPLICATION_JSON_VALUE], produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getPropertyTypeIds(
            @RequestBody propertyTypeFqns: Set<FullQualifiedName>
    ): Map<FullQualifiedName, UUID> {
        return LegacyEdmResolver.getPropertyTypeIds(propertyTypeFqns)
    }

    @RequestMapping(
            path = [ChronicleApi.ORGANIZATION_ID_PATH + ChronicleApi.SETTINGS_PATH], method = [RequestMethod.GET],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getAppSettings(
            @PathVariable(ChronicleApi.ORGANIZATION_ID) organizationId: UUID,
            @RequestParam(value = ChronicleApi.APP_NAME) appName: String
    ): Map<String, Any> {
        return when (val appComponent = AppComponent.fromString(appName)) {
            AppComponent.CHRONICLE_DATA_COLLECTION -> LegacyUtil.mapToLegacySettings(
                    organizationSettingsManager.getOrganizationSettings(organizationId).chronicleDataCollection
            )
            else -> organizationSettingsManager
                    .getOrganizationSettings(organizationId)
                    .appSettings.getValue(appComponent)
        }
    }

    @Timed
    @RequestMapping(path = [ChronicleApi.STATUS_PATH], method = [RequestMethod.GET])
    override fun isRunning(): Boolean {
        return true
    }
}
