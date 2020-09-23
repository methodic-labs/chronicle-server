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
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.access.AccessDeniedException;
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

import static com.openlattice.chronicle.constants.EdmConstants.CAFE_ORG_ID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@RestController
@RequestMapping( ChronicleStudyApi.CONTROLLER )
public class ChronicleStudyController implements ChronicleStudyApi {

    private static final Logger logger = LoggerFactory.getLogger( ChronicleStudyController.class );

    private final FullQualifiedName PERSON_ID_FQN = new FullQualifiedName( "nc.SubjectIdentification" );

    private static final String RAW_DATA_PREFIX = "ChronicleData_";
    private static final String PREPROCESSED_DATA_PREFIX = "ChroniclePreprocessedData_";
    private static final String USAGE_DATA_PREFIX = "ChronicleAppUsageData_";

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

        return enrollSource( CAFE_ORG_ID, studyId, participantId, datasourceId, datasource );
    }

    @Override
    @RequestMapping(
            path = ORGANIZATION_ID_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH + DATASOURCE_ID_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public UUID enrollDataSourceInOrgStudy(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @PathVariable( DATASOURCE_ID ) String datasourceId,
            @RequestBody Optional<Datasource> datasource ) {

        return enrollSource( organizationId, studyId, participantId, datasourceId, datasource );
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
        return chronicleService.isKnownDatasource( CAFE_ORG_ID, studyId, participantId, datasourceId );
    }

    @RequestMapping(
            path = AUTHENTICATED + ORGANIZATION_ID_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH,
            method = RequestMethod.DELETE
    )
    @Override
    public Void deleteParticipantAndAllNeighbors(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @RequestParam( TYPE ) DeleteType deleteType
    ) {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        String token = ( (JwtAuthentication) authentication ).getToken();
        chronicleService.deleteParticipantAndAllNeighbors( organizationId, studyId, participantId, deleteType, token );

        return null;
    }

    @RequestMapping(
            path = AUTHENTICATED + ORGANIZATION_ID_PATH + STUDY_ID_PATH,
            method = RequestMethod.DELETE
    )
    @ResponseStatus( HttpStatus.OK )
    public Void deleteStudyAndAllNeighbors(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId,
            @RequestParam( TYPE ) DeleteType deleteType
    ) {

        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        String token = ( (JwtAuthentication) authentication ).getToken();
        chronicleService.deleteStudyAndAllNeighbors( organizationId, studyId, deleteType, token );
        return null;
    }

    @RequestMapping(
            path = ORGANIZATION_ID_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH + APPS,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public List<ChronicleAppsUsageDetails> getParticipantAppsUsageData(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @RequestParam( value = DATE ) String date ) {
        return chronicleService.getParticipantAppsUsageData( organizationId, studyId, participantId, date );
    }

    @Override
    @RequestMapping(
            path = STUDY_ID_PATH + NOTIFICATIONS,
            method = RequestMethod.GET
    )
    public Boolean isNotificationsEnabled(
            @PathVariable( STUDY_ID ) UUID studyId ) {
        return chronicleService.isNotificationsEnabled( CAFE_ORG_ID, studyId );
    }

    @Override
    @RequestMapping(
            path = ORGANIZATION_ID_PATH + STUDY_ID_PATH + NOTIFICATIONS,
            method = RequestMethod.GET
    )
    public Boolean isOrgStudyNotificationsEnabled(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId ) {
        return chronicleService.isNotificationsEnabled( organizationId, studyId );
    }

    @Override
    @RequestMapping(
            path = STUDY_ID_PATH + PARTICIPANT_ID_PATH + ENROLLMENT_STATUS,
            method = RequestMethod.GET
    )
    public ParticipationStatus getParticipationStatus(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId ) {
        return chronicleService.getParticipationStatus( CAFE_ORG_ID, studyId, participantId );
    }

    @Override
    @RequestMapping(
            path = ORGANIZATION_ID_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH + ENROLLMENT_STATUS,
            method = RequestMethod.GET
    )
    public ParticipationStatus getOrgStudyParticipationStatus(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId ) {
        return chronicleService.getParticipationStatus( organizationId, studyId, participantId );
    }

    @RequestMapping(
            path = ORGANIZATION_ID_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH + APPS,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE
    )
    public Integer updateAppsUsageAssociationData(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @RequestBody Map<UUID, Map<FullQualifiedName, Set<Object>>> associationDetails ) {
        return chronicleService
                .updateAppsUsageAssociationData( organizationId, studyId, participantId, associationDetails );
    }

    @RequestMapping(
            path = AUTHENTICATED + PARTICIPANT_PATH + DATA_PATH + ORGANIZATION_ID_PATH + STUDY_ID_PATH
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

        Iterable<Map<String, Set<Object>>> data = getAllPreprocessedParticipantData( organizationId, studyId,
                participantEntityKeyId,
                fileType );

        String fileName = getParticipantDataFileName( PREPROCESSED_DATA_PREFIX,
                organizationId,
                studyId,
                participantEntityKeyId );
        setContentDisposition( response, fileName, fileType );
        setDownloadContentType( response, fileType );
        return data;
    }

    public Iterable<Map<String, Set<Object>>> getAllPreprocessedParticipantData(
            UUID organizationId,
            UUID studyId,
            UUID participantEntityKeyId,
            FileType fileType ) {

        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        String token = ( (JwtAuthentication) authentication ).getToken();
        return chronicleService
                .getAllPreprocessedParticipantData( organizationId, studyId, participantEntityKeyId, token );

    }

    @RequestMapping(
            path = AUTHENTICATED + PARTICIPANT_PATH + DATA_PATH + ORGANIZATION_ID_PATH + STUDY_ID_PATH
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

        Iterable<Map<String, Set<Object>>> data = getAllParticipantData( organizationId,
                studyId,
                participantEntityKeyId,
                fileType );

        String fileName = getParticipantDataFileName( RAW_DATA_PREFIX,
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
        return chronicleService.getAllParticipantData( organizationId, studyId, participantEntityKeyId, token );
    }

    @RequestMapping(
            path = AUTHENTICATED + PARTICIPANT_PATH + DATA_PATH + ORGANIZATION_ID_PATH + STUDY_ID_PATH
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

        Iterable<Map<String, Set<Object>>> data = getAllParticipantAppsUsageData( organizationId, studyId,
                participantEntityKeyId,
                fileType );

        String fileName = getParticipantDataFileName( USAGE_DATA_PREFIX,
                organizationId,
                studyId,
                participantEntityKeyId );
        setContentDisposition( response, fileName, fileType );
        setDownloadContentType( response, fileType );
        return data;
    }

    @RequestMapping(
            path = ORGANIZATION_ID_PATH + STUDY_ID_PATH + QUESTIONNAIRE + ENTITY_KEY_ID_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    @Override
    public ChronicleQuestionnaire getChronicleQuestionnaire(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( ENTITY_KEY_ID ) UUID questionnaireEKID
    ) {
        return chronicleService.getQuestionnaire( organizationId, studyId, questionnaireEKID );
    }

    @RequestMapping(
            path = ORGANIZATION_ID_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH + QUESTIONNAIRE,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE
    )
    @Override
    public void submitQuestionnaire(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @RequestBody Map<UUID, Map<FullQualifiedName, Set<Object>>> questionnaireResponses ) {
        chronicleService.submitQuestionnaire( organizationId, studyId, participantId, questionnaireResponses );
    }

    @Override
    @RequestMapping(
            path = STUDY_ID_PATH + QUESTIONNAIRES,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public Map<UUID, Map<FullQualifiedName, Set<Object>>> getStudyQuestionnaires(
            @PathVariable( STUDY_ID ) UUID studyId ) {
        return chronicleService.getStudyQuestionnaires( CAFE_ORG_ID, studyId );
    }

    @Override
    @RequestMapping(
            path = ORGANIZATION_ID_PATH + STUDY_ID_PATH + QUESTIONNAIRES,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public Map<UUID, Map<FullQualifiedName, Set<Object>>> getOrgStudyQuestionnaires(
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId ) {
        return chronicleService.getStudyQuestionnaires( organizationId, studyId );
    }

    public Iterable<Map<String, Set<Object>>> getAllParticipantAppsUsageData(
            UUID organizationId,
            UUID studyId,
            UUID participantEntityKeyId,
            FileType fileType ) {

        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        String token = ( (JwtAuthentication) authentication ).getToken();
        return chronicleService
                .getAllParticipantAppsUsageData( organizationId, studyId, participantEntityKeyId, token );

    }

    private UUID enrollSource(
            UUID organizationId,
            UUID studyId,
            String participantId,
            String dataSourceId,
            Optional<Datasource> datasource ) {
        //  allow to proceed only if the participant is in the study and the device has not been associated yet
        final boolean isKnownParticipant = chronicleService.isKnownParticipant( organizationId, studyId, participantId );
        final UUID deviceEKID = chronicleService.getDeviceEntityKeyId( organizationId, studyId, participantId, dataSourceId );

        logger.info(
                "Attempting to enroll source... study {}, participant {}, and datasource {} ",
                studyId,
                participantId,
                dataSourceId
        );
        logger.info( "isKnownParticipant {} = {}", participantId, isKnownParticipant );
        logger.info( "isKnownDatasource {} = {}", dataSourceId, deviceEKID != null );

        if ( isKnownParticipant && deviceEKID == null ) {
            return chronicleService
                    .registerDatasource( organizationId, studyId, participantId, dataSourceId, datasource );
        } else if ( isKnownParticipant ) {
            return deviceEKID;
        } else {
            logger.error(
                    "Unable to enroll device for orgId {} study {}, participant {}, and datasource {} due valid participant = {} or valid device = {}",
                    organizationId,
                    studyId,
                    participantId,
                    dataSourceId,
                    isKnownParticipant,
                    deviceEKID != null );
            throw new AccessDeniedException( "Unable to enroll device." );
        }
    }

    private String getParticipantDataFileName(
            String fileNamePrefix,
            UUID organizationId,
            UUID studyId,
            UUID participantEntityKeyId ) {
        String participantId = chronicleService
                .getParticipantEntity( organizationId, studyId, participantEntityKeyId )
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
