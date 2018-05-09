package com.openlattice.chronicle.controllers;

import com.google.common.collect.SetMultimap;
import com.openlattice.chronicle.ChronicleApi;
import com.openlattice.chronicle.services.ChronicleService;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping( ChronicleApi.CONTROLLER )
public class ChronicleController implements ChronicleApi {

    private static final Logger logger = LoggerFactory.getLogger( ChronicleController.class );

    @Inject
    private ChronicleService chronicleService;

    @Override
    @RequestMapping(
            path = STUDY_ID_PATH + PARTICIPANT_ID_PATH + DATASOURCE_ID_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public Integer upload(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @PathVariable( DATASOURCE_ID ) String datasourceId,
            @RequestBody List<SetMultimap<UUID, Object>> data ) {
        //  allow to proceed only if the participant is in the study and the device is associated as well
        //  TODO: finish exception logic
        final boolean knownParticipant = chronicleService.isKnownParticipant( studyId, participantId );
        final boolean knownDatasource = chronicleService.isKnownDatasource( studyId, participantId, datasourceId );
        if ( knownParticipant && knownDatasource ) {
            return chronicleService.logData( studyId, participantId, datasourceId, data );
        } else {
            logger.error(
                    "Unable to log information for study {}, participant {}, and datasource {} due valid participant = {} or valid device = {}",
                    studyId,
                    participantId,
                    datasourceId,
                    knownParticipant,
                    knownDatasource );
            throw new AccessDeniedException( "Unable to store uploaded data." );
        }
    }

    @Override
    @RequestMapping(
            path = EDM_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Map<String, UUID> getPropertyTypeIds( @RequestBody Set<String> propertyTypeFqns ) {
        return chronicleService.getPropertyTypeIds( propertyTypeFqns );
    }

}
