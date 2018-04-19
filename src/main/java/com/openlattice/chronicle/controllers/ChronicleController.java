package com.openlattice.chronicle.controllers;

import com.google.common.collect.SetMultimap;
import com.openlattice.chronicle.services.ChronicleService;
import com.openlattice.chronicle.ChronicleApi;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import java.util.UUID;

@RestController
@RequestMapping( ChronicleApi.CONTROLLER )
public class ChronicleController implements ChronicleApi {

    @Inject
    private ChronicleService chronicleService;

    @Override
    @RequestMapping(
            path = STUDY_ID_PATH + PARTICIPANT_ID_PATH ,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public void logData(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) UUID participantId,
            @RequestBody SetMultimap<UUID, Object> data) {
        chronicleService.logData( studyId, participantId, data );
    }

    @Override
    @RequestMapping(
            path = STUDY_ID + PARTICIPANT_ID + DEVICE_ID,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public void enrollDevice( @PathVariable( STUDY_ID) UUID studyId,
                              @PathVariable( PARTICIPANT_ID ) UUID participantId,
                              @PathVariable( DEVICE_ID ) String deviceId) {
        chronicleService.enrollDevice( studyId, participantId, deviceId );
    }
}
