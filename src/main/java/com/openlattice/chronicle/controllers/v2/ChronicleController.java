package com.openlattice.chronicle.controllers.v2;

import com.auth0.spring.security.api.authentication.JwtAuthentication;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.google.common.collect.SetMultimap;
import com.openlattice.chronicle.api.ChronicleApi;
import com.openlattice.chronicle.constants.*;
import com.openlattice.chronicle.data.*;
import com.openlattice.chronicle.services.delete.DataDeletionManager;
import com.openlattice.chronicle.services.download.DataDownloadManager;
import com.openlattice.chronicle.services.enrollment.EnrollmentManager;
import com.openlattice.chronicle.services.surveys.SurveysManager;
import com.openlattice.chronicle.services.upload.AppDataUploadManager;
import com.openlattice.chronicle.sources.Datasource;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.http.MediaType;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.openlattice.chronicle.util.ChronicleServerUtil.getParticipantDataFileName;
import static com.openlattice.chronicle.util.ChronicleServerUtil.setContentDisposition;
import static com.openlattice.chronicle.util.ChronicleServerUtil.setDownloadContentType;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
@RestController
@RequestMapping( ChronicleApi.CONTROLLER )
public class ChronicleController implements ChronicleApi {

    private static final String RAW_DATA_PREFIX          = "ChronicleData_";
    private static final String PREPROCESSED_DATA_PREFIX = "ChroniclePreprocessedData_";
    private static final String USAGE_DATA_PREFIX        = "ChronicleAppUsageData_";

    @Inject
    private AppDataUploadManager dataUploadManager;

    @Inject
    private DataDownloadManager dataDownloadManager;

    @Inject
    private EnrollmentManager enrollmentManager;

    @Inject
    private DataDeletionManager dataDeletionManager;

    @Inject
    private SurveysManager surveysManager;

    @Override
    @Timed
    @RequestMapping(
            path = ORGANIZATION_ID_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH + DATASOURCE_ID_PATH + ENROLL_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public UUID enrollSource(
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
            path = AUTHENTICATED_PATH + ORGANIZATION_ID_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH,
            method = RequestMethod.DELETE
    )
    public Void deleteParticipantAndAllNeighbors(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @RequestParam( TYPE ) DeleteType deleteType
    ) {

        String token = getTokenFromContext();
        dataDeletionManager
                .deleteParticipantAndAllNeighbors( organizationId, studyId, participantId, deleteType, token );

        return null;
    }

    @Override
    @Timed
    @RequestMapping(
            path = AUTHENTICATED_PATH + ORGANIZATION_ID_PATH + STUDY_ID_PATH,
            method = RequestMethod.DELETE
    )
    public Void deleteStudyAndAllNeighbors(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId,
            @RequestParam( TYPE ) DeleteType deleteType
    ) {

        String token = getTokenFromContext();
        dataDeletionManager.deleteStudyAndAllNeighbors( organizationId, studyId, deleteType, token );

        return null;
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
    public Iterable<Map<String, Set<Object>>> getAllPreprocessedParticipantData(
            UUID organizationId,
            UUID studyId,
            UUID participantEntityKeyId,
            FileType fileType ) {

        String token = getTokenFromContext();

        return dataDownloadManager
                .getAllPreprocessedParticipantData( organizationId, studyId, participantEntityKeyId, token );
    }

    @Timed
    @RequestMapping(
            path = AUTHENTICATED_PATH + PARTICIPANT_PATH + DATA_PATH + ORGANIZATION_ID_PATH + STUDY_ID_PATH
                    + ENTITY_KEY_ID_PATH
                    + PREPROCESSED_PATH,
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE }
    )
    public Iterable<Map<String, Set<Object>>> getAllPreprocessedParticipantData(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( ENTITY_KEY_ID ) UUID participantEntityKeyId,
            @RequestParam( value = FILE_TYPE, required = false ) FileType fileType,
            HttpServletResponse response ) {

        Iterable<Map<String, Set<Object>>> data = getAllPreprocessedParticipantData(
                organizationId,
                studyId,
                participantEntityKeyId,
                fileType
        );

        String fileName = getParticipantDataFileName(
                enrollmentManager,
                PREPROCESSED_DATA_PREFIX,
                organizationId,
                studyId,
                participantEntityKeyId );
        setContentDisposition( response, fileName, fileType );
        setDownloadContentType( response, fileType );
        return data;
    }

    @Override
    public Iterable<Map<String, Set<Object>>> getAllParticipantData(
            UUID organizationId,
            UUID studyId,
            UUID participantEntityKeyId,
            FileType fileType ) {

        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        String token = ( (JwtAuthentication) authentication ).getToken();

        return dataDownloadManager.getAllParticipantData( organizationId, studyId, participantEntityKeyId, token );
    }

    @Timed
    @RequestMapping(
            path = AUTHENTICATED_PATH + PARTICIPANT_PATH + DATA_PATH + ORGANIZATION_ID_PATH + STUDY_ID_PATH
                    + ENTITY_KEY_ID_PATH,
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE }
    )
    public Iterable<Map<String, Set<Object>>> getAllParticipantData(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( ENTITY_KEY_ID ) UUID participantEntityKeyId,
            @RequestParam( value = FILE_TYPE, required = false ) FileType fileType,
            HttpServletResponse response ) {

        Iterable<Map<String, Set<Object>>> data = getAllParticipantData(
                organizationId,
                studyId,
                participantEntityKeyId,
                fileType
        );

        String fileName = getParticipantDataFileName(
                enrollmentManager,
                RAW_DATA_PREFIX,
                organizationId,
                studyId,
                participantEntityKeyId );
        setContentDisposition( response, fileName, fileType );
        setDownloadContentType( response, fileType );
        return data;
    }

    @Override
    public Iterable<Map<String, Set<Object>>> getAllParticipantAppsUsageData(
            UUID organizationId, UUID studyId, UUID participantEntityKeyId, FileType fileType ) {

        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        String token = ( (JwtAuthentication) authentication ).getToken();

        return dataDownloadManager
                .getAllParticipantAppsUsageData( organizationId, studyId, participantEntityKeyId, token );
    }

    @Timed
    @RequestMapping(
            path = AUTHENTICATED_PATH + PARTICIPANT_PATH + DATA_PATH + ORGANIZATION_ID_PATH + STUDY_ID_PATH
                    + ENTITY_KEY_ID_PATH + USAGE_PATH,
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE }
    )
    public Iterable<Map<String, Set<Object>>> getAllParticipantAppsUsageData(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( ENTITY_KEY_ID ) UUID participantEntityKeyId,
            @RequestParam( value = FILE_TYPE, required = false ) FileType fileType,
            HttpServletResponse response ) {

        Iterable<Map<String, Set<Object>>> data = getAllParticipantAppsUsageData(
                organizationId,
                studyId,
                participantEntityKeyId,
                fileType );

        String fileName = getParticipantDataFileName(
                enrollmentManager,
                USAGE_DATA_PREFIX,
                organizationId,
                studyId,
                participantEntityKeyId );
        setContentDisposition( response, fileName, fileType );
        setDownloadContentType( response, fileType );
        return data;
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
    public Map<String, UUID> getPropertyTypeIds( Set<String> propertyTypeFqns ) {
        return enrollmentManager.getPropertyTypeIds( propertyTypeFqns );
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

    // controller helper methods
    private String getTokenFromContext() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        return (( JwtAuthentication) authentication).getToken();
    }
}
