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

import com.google.common.base.Optional;
import com.google.common.collect.SetMultimap;
import com.openlattice.chronicle.ChronicleStudyApi;
import com.openlattice.chronicle.constants.CustomMediaType;
import com.openlattice.chronicle.data.FileType;
import com.openlattice.chronicle.services.ChronicleService;
import com.openlattice.chronicle.sources.Datasource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@RestController
@RequestMapping( ChronicleStudyApi.CONTROLLER )
public class ChronicleStudyController implements ChronicleStudyApi {
    private static final Logger logger = LoggerFactory.getLogger( ChronicleStudyController.class );

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

        if ( knownParticipant && !knownDatasource ) {
            return chronicleService.registerDatasource( studyId, participantId, datasourceId, datasource );
        } else if ( knownParticipant && knownDatasource ) {
            return chronicleService.getDatasourceEntityKeyId( datasourceId );
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

    @RequestMapping(
            path = PARTICIPANT_PATH + DATA_PATH + STUDY_ID_PATH + ENTITY_ID_PATH,
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE } )
    public Iterable<SetMultimap<String, Object>> getAllParticipantData(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( ENTITY_ID ) UUID participantEntityId,
            @RequestParam( value = FILE_TYPE, required = false ) FileType fileType,
            HttpServletResponse response ) {

        setContentDisposition( response, participantEntityId.toString(), fileType );
        setDownloadContentType( response, fileType );

        return getAllParticipantData( studyId, participantEntityId, fileType );
    }

    @Override
    public Iterable<SetMultimap<String, Object>> getAllParticipantData(
            UUID studyId,
            UUID participantEntityId,
            FileType fileType ) {

        return chronicleService.getAllParticipantData( studyId, participantEntityId );
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

        if ( fileType == FileType.yaml || fileType == FileType.json ) {
            response.setHeader(
                    "Content-Disposition",
                    "attachment; filename=" + fileName + "." + fileType.toString()
            );
        }
    }
}
