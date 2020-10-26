/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 */

package com.openlattice.chronicle.controllers;

import com.auth0.spring.security.api.authentication.JwtAuthentication;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.openlattice.chronicle.ChronicleStudyApi;
import com.openlattice.chronicle.constants.*;
import com.openlattice.chronicle.data.*;
import com.openlattice.chronicle.services.delete.DataDeletionManager;
import com.openlattice.chronicle.services.download.DataDownloadManager;
import com.openlattice.chronicle.services.enrollment.EnrollmentManager;
import com.openlattice.chronicle.services.surveys.SurveysManager;
import com.openlattice.chronicle.services.upload.AppDataUploadManager;
import com.openlattice.chronicle.sources.Datasource;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.http.HttpStatus;
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
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@RestController
@RequestMapping( ChronicleStudyApi.CONTROLLER )
public class ChronicleStudyController implements ChronicleStudyApi {

    private static final String RAW_DATA_PREFIX          = "ChronicleData_";
    private static final String PREPROCESSED_DATA_PREFIX = "ChroniclePreprocessedData_";
    private static final String USAGE_DATA_PREFIX        = "ChronicleAppUsageData_";

    @Inject
    private DataDeletionManager dataDeletionManager;

    @Inject
    private AppDataUploadManager dataUploadManager;

    @Inject
    private SurveysManager surveysManager;

    @Inject
    private DataDownloadManager dataDownloadManager;

    @Inject
    private EnrollmentManager enrollmentManager;

    @Override
    @Timed
    @RequestMapping(
            path = STUDY_ID_PATH + PARTICIPANT_ID_PATH + DATASOURCE_ID_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE
    )
    public UUID enrollSource(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @PathVariable( DATASOURCE_ID ) String datasourceId,
            @RequestBody Optional<Datasource> datasource ) {

        return enrollmentManager.registerDatasource( null, studyId, participantId, datasourceId, datasource );
    }

    @Override
    @Timed
    @RequestMapping(
            path = STUDY_ID_PATH + PARTICIPANT_ID_PATH + DATASOURCE_ID_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public Boolean isKnownDatasource(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @PathVariable( DATASOURCE_ID ) String datasourceId ) {
        //  validate that this device belongs to this participant in this study
        //  look up in association entitySet between device and participant, and device and study to see if it exists?
        //  DataApi.getEntity(entitySetId :UUID, entityKeyId :UUID)
        return enrollmentManager.isKnownDatasource( null, studyId, participantId, datasourceId );
    }

    @Override
    @Timed
    @RequestMapping(
            path = AUTHENTICATED + STUDY_ID_PATH + PARTICIPANT_ID_PATH,
            method = RequestMethod.DELETE
    )
    public Void deleteParticipantAndAllNeighbors(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @RequestParam( TYPE ) DeleteType deleteType
    ) {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        String token = ( (JwtAuthentication) authentication ).getToken();
        dataDeletionManager.deleteParticipantAndAllNeighbors( null, studyId, participantId, deleteType, token );

        return null;
    }

    @Timed
    @RequestMapping(
            path = AUTHENTICATED + STUDY_ID_PATH,
            method = RequestMethod.DELETE
    )
    @ResponseStatus( HttpStatus.OK )
    public Void deleteStudyAndAllNeighbors(
            @PathVariable( STUDY_ID ) UUID studyId,
            @RequestParam( TYPE ) DeleteType deleteType
    ) {

        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        String token = ( (JwtAuthentication) authentication ).getToken();
        dataDeletionManager.deleteStudyAndAllNeighbors( null, studyId, deleteType, token );
        return null;
    }

    @Timed
    @Override
    @RequestMapping(
            path = PARTICIPANT_PATH + DATA_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH + APPS,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public List<ChronicleAppsUsageDetails> getParticipantAppsUsageData(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @RequestParam( value = DATE ) String date ) {
        return surveysManager.getParticipantAppsUsageData( null, studyId, participantId, date );
    }

    @Override
    @Timed
    @RequestMapping(
            path = STUDY_ID_PATH + NOTIFICATIONS,
            method = RequestMethod.GET
    )
    public Boolean isNotificationsEnabled(
            @PathVariable( STUDY_ID ) UUID studyId ) {
        return enrollmentManager.isNotificationsEnabled( null, studyId );
    }

    @Override
    @Timed
    @RequestMapping(
            path = STUDY_ID_PATH + PARTICIPANT_ID_PATH + ENROLLMENT_STATUS,
            method = RequestMethod.GET
    )
    public ParticipationStatus getParticipationStatus(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId ) {
        return enrollmentManager.getParticipationStatus( null, studyId, participantId );
    }

    @Override
    @RequestMapping(
            path = PARTICIPANT_PATH + DATA_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH + APPS,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE
    )
    public void submitAppUsageSurvey(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @RequestBody Map<UUID, Map<FullQualifiedName, Set<Object>>> associationDetails ) {

        surveysManager.submitAppUsageSurvey( null, studyId, participantId, associationDetails );
    }

    @Timed
    @RequestMapping(
            path = AUTHENTICATED + PARTICIPANT_PATH + DATA_PATH + STUDY_ID_PATH
                    + ENTITY_KEY_ID_PATH
                    + PREPROCESSED_PATH,
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE }
    )
    public Iterable<Map<String, Set<Object>>> getAllPreprocessedParticipantData(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( ENTITY_KEY_ID ) UUID participantEntityKeyId,
            @RequestParam( value = FILE_TYPE, required = false ) FileType fileType,
            HttpServletResponse response ) {

        Iterable<Map<String, Set<Object>>> data = getAllPreprocessedParticipantData(
                studyId,
                participantEntityKeyId,
                fileType
        );

        String fileName = getParticipantDataFileName(
                enrollmentManager,
                PREPROCESSED_DATA_PREFIX,
                null,
                studyId,
                participantEntityKeyId );
        setContentDisposition( response, fileName, fileType );
        setDownloadContentType( response, fileType );
        return data;
    }

    public Iterable<Map<String, Set<Object>>> getAllPreprocessedParticipantData(
            UUID studyId,
            UUID participantEntityKeyId,
            FileType fileType ) {

        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        String token = ( (JwtAuthentication) authentication ).getToken();
        return dataDownloadManager.getAllPreprocessedParticipantData( null, studyId, participantEntityKeyId, token );

    }

    @Timed
    @RequestMapping(
            path = AUTHENTICATED + PARTICIPANT_PATH + DATA_PATH + STUDY_ID_PATH
                    + ENTITY_KEY_ID_PATH,
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE }
    )
    public Iterable<Map<String, Set<Object>>> getAllParticipantData(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( ENTITY_KEY_ID ) UUID participantEntityKeyId,
            @RequestParam( value = FILE_TYPE, required = false ) FileType fileType,
            HttpServletResponse response ) {

        Iterable<Map<String, Set<Object>>> data = getAllParticipantData(
                studyId,
                participantEntityKeyId,
                fileType
        );

        String fileName = getParticipantDataFileName(
                enrollmentManager,
                RAW_DATA_PREFIX,
                null,
                studyId,
                participantEntityKeyId );
        setContentDisposition( response, fileName, fileType );
        setDownloadContentType( response, fileType );
        return data;
    }

    @Override
    public Iterable<Map<String, Set<Object>>> getAllParticipantData(
            UUID studyId,
            UUID participantEntityKeyId,
            FileType fileType ) {

        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        String token = ( (JwtAuthentication) authentication ).getToken();
        return dataDownloadManager.getAllParticipantData( null, studyId, participantEntityKeyId, token );
    }

    @Timed
    @RequestMapping(
            path = AUTHENTICATED + PARTICIPANT_PATH + DATA_PATH + STUDY_ID_PATH
                    + ENTITY_KEY_ID_PATH + USAGE_PATH,
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE }
    )
    public Iterable<Map<String, Set<Object>>> getAllParticipantAppsUsageData(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( ENTITY_KEY_ID ) UUID participantEntityKeyId,
            @RequestParam( value = FILE_TYPE, required = false ) FileType fileType,
            HttpServletResponse response ) {

        Iterable<Map<String, Set<Object>>> data = getAllParticipantAppsUsageData(
                studyId,
                participantEntityKeyId,
                fileType );

        String fileName = getParticipantDataFileName(
                enrollmentManager,
                USAGE_DATA_PREFIX,
                null,
                studyId,
                participantEntityKeyId );
        setContentDisposition( response, fileName, fileType );
        setDownloadContentType( response, fileType );
        return data;
    }

    @Override
    @Timed
    @RequestMapping(
            path = STUDY_ID_PATH + QUESTIONNAIRE + ENTITY_KEY_ID_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ChronicleQuestionnaire getChronicleQuestionnaire(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( ENTITY_KEY_ID ) UUID questionnaireEKID
    ) {
        return surveysManager.getQuestionnaire( null, studyId, questionnaireEKID );
    }

    @Override
    @Timed
    @RequestMapping(
            path = STUDY_ID_PATH + PARTICIPANT_ID_PATH + QUESTIONNAIRE,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE
    )
    public void submitQuestionnaire(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @RequestBody Map<UUID, Map<FullQualifiedName, Set<Object>>> questionnaireResponses ) {

        surveysManager.submitQuestionnaire( null, studyId, participantId, questionnaireResponses );
    }

    @Override
    @Timed
    @RequestMapping(
            path = STUDY_ID_PATH + QUESTIONNAIRES,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public Map<UUID, Map<FullQualifiedName, Set<Object>>> getStudyQuestionnaires(
            @PathVariable( STUDY_ID ) UUID studyId ) {

        return surveysManager.getStudyQuestionnaires( null, studyId );
    }

    @Override
    public Iterable<Map<String, Set<Object>>> getAllParticipantAppsUsageData(
            UUID studyId,
            UUID participantEntityKeyId,
            FileType fileType ) {

        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        String token = ( (JwtAuthentication) authentication ).getToken();

        return dataDownloadManager.getAllParticipantAppsUsageData( null, studyId, participantEntityKeyId, token );
    }

    @RequestMapping(
            path = STUDY_ID_PATH + PARTICIPANT_ID_PATH + TIME_USE_DIARY,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE
    )
    @Override
    public void submitTimeUseDiarySurvey(
            @PathVariable ( STUDY_ID ) UUID studyId,
            @PathVariable ( PARTICIPANT_ID ) String participantId,
            @RequestBody List<Map<FullQualifiedName, Set<Object>>> surveyData
    ) {
        surveysManager.submitTimeUseDiarySurvey( null, studyId, participantId, surveyData );
    }
}
