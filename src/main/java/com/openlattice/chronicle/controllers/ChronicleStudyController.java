package com.openlattice.chronicle.controllers;

import com.google.common.base.Optional;
import com.openlattice.chronicle.ChronicleStudyApi;
import com.openlattice.chronicle.services.ChronicleService;
import com.openlattice.chronicle.sources.Datasource;
import java.util.UUID;
import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

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
            throw new ForbiddenException( "Unable to enroll device." );
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
}
