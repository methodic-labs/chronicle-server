package com.openlattice.chronicle.controllers;

import com.google.common.collect.SetMultimap;
import com.openlattice.chronicle.ChronicleApi;
import com.openlattice.chronicle.data.ParticipationStatus;
import com.openlattice.chronicle.services.ChronicleService;
import com.openlattice.chronicle.util.ChronicleServerExceptionHandler.StudyRegistrationNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.openlattice.chronicle.constants.EdmConstants.CAFE_ORG_ID;

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

        return uploadData(CAFE_ORG_ID, studyId, participantId, datasourceId, data);
    }

    @Override
    @RequestMapping(
            path = ORGANIZATION_ID_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH + DATASOURCE_ID_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public Integer uploadV2(
            @PathVariable ( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @PathVariable( DATASOURCE_ID ) String datasourceId,
            @RequestBody List<SetMultimap<UUID, Object>> data ) {

        return uploadData(organizationId, studyId, participantId, datasourceId, data);
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

    @RequestMapping(
            path = STATUS_PATH,
            method = RequestMethod.GET )
    @Override public Boolean isRunning() {
        //TODO: Ensure connectivity with OpenLattice backend.
        return true;
    }

    private Integer uploadData(
            UUID organizationId,
            UUID studyId,
            String participantId,
            String dataSourceId,
            List<SetMultimap<UUID, Object>> data ) {
        //  allow to proceed only if the participant is in the study and the device is associated as well
        //  TODO: finish exception logic
        final boolean knownParticipant = chronicleService.isKnownParticipant( organizationId, studyId, participantId );
        final boolean knownDatasource = chronicleService.isKnownDatasource( organizationId, studyId, participantId, dataSourceId );

        final ParticipationStatus status = chronicleService.getParticipationStatus( organizationId, studyId, participantId );
        if ( ParticipationStatus.NOT_ENROLLED.equals( status ) ) {
            logger.warn( "participantId = {} is not enrolled, ignoring data upload", participantId );
            return 0;
        }

        if ( knownParticipant && knownDatasource ) {
            return chronicleService.logData( organizationId, studyId, participantId, dataSourceId, data );
        } else {
            logger.error(
                    "Unable to log information for study {}, participant {}, and datasource {} due valid participant = {} or valid device = {}",
                    studyId,
                    participantId,
                    dataSourceId,
                    knownParticipant,
                    knownDatasource );
            if ( !knownParticipant ) {
                throw new AccessDeniedException( "Unable to store uploaded data." );
            }

            throw new StudyRegistrationNotFoundException( "Unable to store uploaded data." );
        }
    }
}
