package com.openlattice.chronicle.controllers;

import com.google.common.base.Optional;
import com.openlattice.chronicle.ChronicleStudyApi;
import com.openlattice.chronicle.constants.CustomMediaType;
import com.openlattice.chronicle.services.ChronicleService;
import com.openlattice.chronicle.sources.Datasource;
import com.openlattice.chronicle.sources.EntitySetData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.ForbiddenException;
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

    @Override
    public EntitySetData getAllStudyData( UUID studyId, String token ) {
        return chronicleService.downloadStudyData( studyId, token );
    }

    @RequestMapping(
            path = STUDY_ID_PATH + DATA_PATH,
            method = RequestMethod.GET )
    public EntitySetData getAllStudyData(
            @PathVariable( STUDY_ID ) UUID studyId,
            HttpServletRequest request,
            HttpServletResponse response ) {
        String authHeader = request.getHeader( "Authorization" );
        if ( authHeader == null ) {
            throw new ForbiddenException( "Missing Authentication header." );
        }

        String[] tokenSplit = authHeader.split( " " );
        if ( tokenSplit.length <= 1 ) {
            throw new ForbiddenException( "Invalid Authentication header." );
        }

        String token = tokenSplit[ 1 ];
        response.setHeader( "Content-Disposition",
                "attachment; filename=" + studyId.toString() + ".csv" );
        response.setContentType( CustomMediaType.TEXT_CSV_VALUE );
        return getAllStudyData( studyId, token );
    }
}
