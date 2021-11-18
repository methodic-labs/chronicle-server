package com.openlattice.chronicle.controllers.v2;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.google.common.collect.SetMultimap;
import com.openlattice.chronicle.api.ChronicleApi;
import com.openlattice.chronicle.data.ChronicleAppsUsageDetails;
import com.openlattice.chronicle.data.ChronicleQuestionnaire;
import com.openlattice.chronicle.data.MessageDetails;
import com.openlattice.chronicle.data.ParticipationStatus;
import com.openlattice.chronicle.services.edm.EdmCacheManager;
import com.openlattice.chronicle.services.enrollment.EnrollmentManager;
import com.openlattice.chronicle.services.entitysets.EntitySetIdsManager;
import com.openlattice.chronicle.services.message.MessageService;
import com.openlattice.chronicle.services.surveys.SurveysManager;
import com.openlattice.chronicle.services.upload.AppDataUploadManager;
import com.openlattice.chronicle.sources.Datasource;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
@RestController
@RequestMapping( ChronicleApi.CONTROLLER )
public class ChronicleControllerV2 implements ChronicleApi {

    @Inject
    private AppDataUploadManager dataUploadManager;

    @Inject
    private EnrollmentManager enrollmentManager;

    @Inject
    private SurveysManager surveysManager;

    @Inject
    private EdmCacheManager edmCacheManager;

    @Inject
    private EntitySetIdsManager entitySetIdsManager;

    @Inject
    private MessageService messageService;

    @Override
    @Timed
    @RequestMapping(
            path = ORGANIZATION_ID_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH + DATASOURCE_ID_PATH + ENROLL_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public UUID enroll(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @PathVariable( DATASOURCE_ID ) String datasourceId,
            @RequestBody Optional<Datasource> datasource ) {
        return enrollmentManager.registerDatasource( organizationId, studyId, participantId, datasourceId, datasource );
    }

    @Override
    @Timed
    @RequestMapping(
            path = ORGANIZATION_ID_PATH + STUDY_ID_PATH + NOTIFICATIONS_PATH,
            method = RequestMethod.GET
    )
    public Boolean isNotificationsEnabled(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId ) {

        return enrollmentManager.isNotificationsEnabled( organizationId, studyId );
    }

    @Override
    @Timed
    @RequestMapping(
            path = ORGANIZATION_ID_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH + ENROLLMENT_STATUS_PATH,
            method = RequestMethod.GET
    )
    public ParticipationStatus getParticipationStatus(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId ) {

        return enrollmentManager.getParticipationStatus( organizationId, studyId, participantId );
    }

    @Override
    @Timed
    @RequestMapping(
            path = ORGANIZATION_ID_PATH + STUDY_ID_PATH + QUESTIONNAIRE_PATH + ENTITY_KEY_ID_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ChronicleQuestionnaire getChronicleQuestionnaire(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( ENTITY_KEY_ID ) UUID questionnaireEKID ) {

        return surveysManager.getQuestionnaire( organizationId, studyId, questionnaireEKID );
    }

    @Override
    @Timed
    @RequestMapping(
            path = ORGANIZATION_ID_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH + APPS_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE
    )
    public void submitAppUsageSurvey(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @RequestBody Map<UUID, Map<FullQualifiedName, Set<Object>>> associationDetails ) {

        surveysManager.submitAppUsageSurvey( organizationId, studyId, participantId, associationDetails );
    }

    @Override
    @Timed
    @RequestMapping(
            path = ORGANIZATION_ID_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH + QUESTIONNAIRE_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE
    )
    public void submitQuestionnaire(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @RequestBody Map<UUID, Map<FullQualifiedName, Set<Object>>> questionnaireResponses ) {

        surveysManager.submitQuestionnaire( organizationId, studyId, participantId, questionnaireResponses );
    }

    @Timed
    @RequestMapping(
            path = ORGANIZATION_ID_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH + APPS_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public List<ChronicleAppsUsageDetails> getParticipantAppsUsageData(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @RequestParam( value = DATE ) String date ) {

        return surveysManager.getParticipantAppsUsageData( organizationId, studyId, participantId, date );
    }

    @Override
    @Timed
    @RequestMapping(
            path = ORGANIZATION_ID_PATH + STUDY_ID_PATH + QUESTIONNAIRES_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public Map<UUID, Map<FullQualifiedName, Set<Object>>> getStudyQuestionnaires(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId ) {

        return surveysManager.getStudyQuestionnaires( organizationId, studyId );
    }
    
    @RequestMapping(
            path = ORGANIZATION_ID_PATH + MESSAGE_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE
    )
    public void sendMessages(
            @PathVariable ( ORGANIZATION_ID ) UUID organizationId,
            @RequestBody List<MessageDetails> messageDetails
    ) {
        messageService.sendMessages( organizationId, messageDetails);
    }

    @RequestMapping(
            path = ORGANIZATION_ID_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH + TIME_USE_DIARY,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE
    )
    @Override
    public void submitTimeUseDiarySurvey(
            @PathVariable ( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable ( STUDY_ID ) UUID studyId,
            @PathVariable ( PARTICIPANT_ID ) String participantId,
            @RequestBody List<Map<FullQualifiedName, Set<Object>>> surveyData
    ) {
        surveysManager.submitTimeUseDiarySurvey( organizationId, studyId, participantId, surveyData );
    }

    @Override
    @Timed
    @RequestMapping(
            path = ORGANIZATION_ID_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH + DATASOURCE_ID_PATH + UPLOAD_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public Integer upload(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @PathVariable( DATASOURCE_ID ) String datasourceId,
            @RequestBody List<SetMultimap<UUID, Object>> data ) {

        return dataUploadManager.upload( organizationId, studyId, participantId, datasourceId, data );
    }

    @Override
    @Timed
    @RequestMapping(
            path = EDM_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public Map<FullQualifiedName, UUID> getPropertyTypeIds( @RequestBody Set<FullQualifiedName> propertyTypeFqns ) {
        return edmCacheManager.getPropertyTypeIds( propertyTypeFqns );
    }

    @Override
    @RequestMapping(
            path = ORGANIZATION_ID_PATH + SETTINGS_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public Map<String, Object> getAppSettings(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @RequestParam( value = APP_NAME ) String appName
    ) {
        return entitySetIdsManager.getOrgAppSettings( appName, organizationId );
    }

    @Override
    @Timed
    @RequestMapping(
            path = STATUS_PATH,
            method = RequestMethod.GET
    )
    public Boolean isRunning() {

        //TODO: Ensure connectivity with OpenLattice backend.
        return true;
    }
}
