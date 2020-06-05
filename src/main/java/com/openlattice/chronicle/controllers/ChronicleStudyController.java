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
import com.google.common.base.Optional;
import com.openlattice.chronicle.ChronicleStudyApi;
import com.openlattice.chronicle.constants.CustomMediaType;
import com.openlattice.chronicle.data.*;
import com.openlattice.chronicle.services.ChronicleService;
import com.openlattice.chronicle.sources.Datasource;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.InsufficientAuthenticationException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@RestController
@RequestMapping( ChronicleStudyApi.CONTROLLER )
public class ChronicleStudyController implements ChronicleStudyApi {

    private static final Logger logger = LoggerFactory.getLogger( ChronicleStudyController.class );

    private final FullQualifiedName PERSON_ID_FQN = new FullQualifiedName( "nc.SubjectIdentification" );

    @Inject
    private ChronicleService chronicleService;

    @Override
    @RequestMapping(
            path = STUDY_ID_PATH + PARTICIPANT_ID_PATH + DATASOURCE_ID_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public UUID enrollSource(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @PathVariable( DATASOURCE_ID ) String datasourceId,
            @RequestBody Optional<Datasource> datasource ) {
        //  allow to proceed only if the participant is in the study and the device has not been associated yet
        //  TODO: finish exception logic
        final boolean knownParticipant = chronicleService.isKnownParticipant( studyId, participantId );
        final boolean knownDatasource = chronicleService.isKnownDatasource( studyId, participantId, datasourceId );

        logger.info(
                "Attempting to enroll source... study {}, participant {}, and datasource {} ",
                studyId,
                participantId,
                datasourceId
        );
        logger.info( "isKnownParticipant {} = {}", participantId, knownParticipant );
        logger.info( "isKnownDatasource {} = {}", datasourceId, knownDatasource );

        if ( knownParticipant && !knownDatasource ) {
            return chronicleService.registerDatasource( studyId, participantId, datasourceId, datasource );
        } else if ( knownParticipant && knownDatasource ) {
            return chronicleService.getDeviceEntityKeyId( studyId, participantId, datasourceId );
        } else {
            logger.error(
                    "Unable to enroll device for study {}, participant {}, and datasource {} due valid participant = {} or valid device = {}",
                    studyId,
                    participantId,
                    datasourceId,
                    knownParticipant,
                    knownDatasource );
            throw new AccessDeniedException( "Unable to enroll device." );
        }
    }

    @Override
    @RequestMapping(
            path = STUDY_ID_PATH + PARTICIPANT_ID_PATH + DATASOURCE_ID_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Boolean isKnownDatasource(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @PathVariable( DATASOURCE_ID ) String datasourceId ) {
        //  validate that this device belongs to this participant in this study
        //  look up in association entitySet between device and participant, and device and study to see if it exists?
        //  DataApi.getEntity(entitySetId :UUID, entityKeyId :UUID)
        // TODO: Waiting on data model to exist, then ready to implement
        return chronicleService.isKnownDatasource( studyId, participantId, datasourceId );
    }

    @Override
    @RequestMapping(
            path = STUDY_ID_PATH + PARTICIPANT_ID_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Boolean isKnownParticipant(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId ) {
        //  validate that this participant belongs in this study
        //  look up in association entitySet between study and participant if the participant is present
        //  DataApi.getEntity(entitySetId :UUID, entityKeyId :UUID)
        // TODO: Waiting on data model to exist, then ready to implement
        return chronicleService.isKnownParticipant( studyId, participantId );
    }

    @Override
    public void deleteParticipantAndAllNeighbors(
            UUID studyId, String participantId, DeleteType deleteType ) {
        // TODO implement this
    }

    @Override
    public void deleteStudyAndAllNeighbors( UUID studyId, DeleteType deleteType ) {
        // TODO implement this
    }

    @RequestMapping(
            path = PARTICIPANT_PATH + DATA_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH + APPS,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public List<ChronicleAppsUsageDetails> getParticipantAppsUsageData(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @RequestParam( value = DATE ) String date ) {
        return chronicleService.getParticipantAppsUsageData( studyId, participantId, date );
    }

    @RequestMapping(
            path = STUDY_ID_PATH + NOTIFICATIONS,
            method = RequestMethod.GET
    )
    public Boolean isNotificationsEnabled(
            @PathVariable( STUDY_ID ) UUID studyId ) {
        return chronicleService.isNotificationsEnabled( studyId );
    }

    @RequestMapping(
            path = STUDY_ID_PATH + PARTICIPANT_ID_PATH + ENROLLMENT_STATUS,
            method = RequestMethod.GET
    )
    @Override
    public ParticipationStatus getParticipationStatus(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId ) {
        return chronicleService.getParticipationStatus( studyId, participantId );
    }

    @RequestMapping(
            path = PARTICIPANT_PATH + DATA_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH + APPS,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE
    )
    public Integer updateAppsUsageAssociationData(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @RequestBody Map<UUID, Map<FullQualifiedName, Set<Object>>> associationDetails ) {
        return chronicleService.updateAppsUsageAssociationData( studyId, participantId, associationDetails );
    }

    @RequestMapping(
            path = PARTICIPANT_PATH + DATA_PATH + STUDY_ID_PATH + ENTITY_KEY_ID_PATH + PREPROCESSED_PATH,
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE }
    )
    public Iterable<Map<String, Set<Object>>> getAllPreprocessedParticipantData(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( ENTITY_KEY_ID ) UUID participantEntityKeyId,
            @RequestParam( value = FILE_TYPE, required = false ) FileType fileType,
            HttpServletResponse response ) {

        Iterable<Map<String, Set<Object>>> data = getAllPreprocessedParticipantData( studyId,
                participantEntityKeyId,
                fileType );

        String fileName = getParticipantDataFileName( "ChroniclePreprocessedData_", studyId, participantEntityKeyId );
        setContentDisposition( response, fileName, fileType );
        setDownloadContentType( response, fileType );
        return data;
    }

    public Iterable<Map<String, Set<Object>>> getAllPreprocessedParticipantData(
            UUID studyId,
            UUID participantEntityKeyId,
            FileType fileType ) {

        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if ( authentication instanceof JwtAuthentication ) {
            String token = ( (JwtAuthentication) authentication ).getToken();
            return chronicleService.getAllPreprocessedParticipantData( studyId, participantEntityKeyId, token );
        }

        throw new InsufficientAuthenticationException( "request is not authenticated" );
    }

    @RequestMapping(
            path = PARTICIPANT_PATH + DATA_PATH + STUDY_ID_PATH + ENTITY_KEY_ID_PATH,
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE }
    )
    public Iterable<Map<String, Set<Object>>> getAllParticipantData(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( ENTITY_KEY_ID ) UUID participantEntityKeyId,
            @RequestParam( value = FILE_TYPE, required = false ) FileType fileType,
            HttpServletResponse response ) {

        Iterable<Map<String, Set<Object>>> data = getAllParticipantData( studyId, participantEntityKeyId, fileType );

        String fileName = getParticipantDataFileName( "ChronicleData_", studyId, participantEntityKeyId );
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
        if ( authentication instanceof JwtAuthentication ) {
            String token = ( (JwtAuthentication) authentication ).getToken();
            return chronicleService.getAllParticipantData( studyId, participantEntityKeyId, token );
        }

        throw new InsufficientAuthenticationException( "request is not authenticated" );
    }

    @RequestMapping(
            path = PARTICIPANT_PATH + DATA_PATH + STUDY_ID_PATH + ENTITY_KEY_ID_PATH + USAGE_PATH,
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE }
    )
    public Iterable<Map<String, Set<Object>>> getAllParticipantAppsUsageData(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( ENTITY_KEY_ID ) UUID participantEntityKeyId,
            @RequestParam( value = FILE_TYPE, required = false ) FileType fileType,
            HttpServletResponse response ) {

        Iterable<Map<String, Set<Object>>> data = getAllParticipantAppsUsageData( studyId,
                participantEntityKeyId,
                fileType );

        String fileName = getParticipantDataFileName( "ChronicleAppUsageData_", studyId, participantEntityKeyId );
        setContentDisposition( response, fileName, fileType );
        setDownloadContentType( response, fileType );
        return data;
    }

    @RequestMapping(
            path = STUDY_ID_PATH + QUESTIONNAIRE +  ENTITY_KEY_ID_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    @Override
    public ChronicleQuestionnaire getChronicleQuestionnaire(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( ENTITY_KEY_ID ) UUID questionnaireEKID
    ) {
        return chronicleService.getQuestionnaire(studyId, questionnaireEKID);
    }

    @RequestMapping(
            path = STUDY_ID_PATH + PARTICIPANT_ID_PATH +  QUESTIONNAIRE,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE
    )
    @Override
    public void submitQuestionnaire(
            @PathVariable (STUDY_ID) UUID studyId,
            @PathVariable (PARTICIPANT_ID) String participantId,
            @RequestBody  Map<UUID, Map<FullQualifiedName, Set<Object>>> questionnaireResponses ) {
        chronicleService.submitQuestionnaire(studyId, participantId, questionnaireResponses);
    }

    @RequestMapping(
            path = STUDY_ID_PATH  + QUESTIONNAIRES + ACTIVE,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    @Override
    public Map<UUID, Map<FullQualifiedName, Set<Object>>> getActiveQuestionnaires(
            @PathVariable (STUDY_ID) UUID studyId
    ) {
        return chronicleService.getActiveQuestionnaires(studyId);
    }

    public Iterable<Map<String, Set<Object>>> getAllParticipantAppsUsageData(
            UUID studyId,
            UUID participantEntityKeyId,
            FileType fileType ) {

        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if ( authentication instanceof JwtAuthentication ) {
            String token = ( (JwtAuthentication) authentication ).getToken();
            return chronicleService.getAllParticipantAppsUsageData( studyId, participantEntityKeyId, token );
        }

        throw new InsufficientAuthenticationException( "request is not authenticated" );
    }

    private String getParticipantDataFileName( String fileNamePrefix, UUID studyId, UUID participantEntityKeyId ) {
        String participantId = chronicleService
                .getParticipantEntity( studyId, participantEntityKeyId )
                .get( PERSON_ID_FQN )
                .stream()
                .findFirst()
                .orElse( "" )
                .toString();
        StringBuilder fileNameBuilder = new StringBuilder()
                .append( fileNamePrefix )
                .append( LocalDate.now().format( DateTimeFormatter.BASIC_ISO_DATE ) )
                .append( "-" )
                .append( participantId );
        return fileNameBuilder.toString();
    }

    private static void setDownloadContentType( HttpServletResponse response, FileType fileType ) {

        if ( fileType == null ) {
            response.setContentType( MediaType.APPLICATION_JSON_VALUE );
            return;
        }

        switch ( fileType ) {
            case csv:
                response.setContentType( CustomMediaType.TEXT_CSV_VALUE );
                break;
            case yaml:
                response.setContentType( CustomMediaType.TEXT_YAML_VALUE );
                break;
            case json:
            default:
                response.setContentType( MediaType.APPLICATION_JSON_VALUE );
                break;
        }
    }

    private static void setContentDisposition( HttpServletResponse response, String fileName, FileType fileType ) {

        if ( fileType == FileType.csv || fileType == FileType.json ) {
            response.setHeader(
                    "Content-Disposition",
                    "attachment; filename=" + fileName + "." + fileType.toString()
            );
        }
    }

}
