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
            path = STUDY_ID_PATH + PARTICIPANT_ID_PATH + DEVICE_ID_PATH + ENTITY_SET_ID_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public void logData(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) UUID participantId,
            @PathVariable( DEVICE_ID ) String deviceId,
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody SetMultimap<UUID, Object> data ) {
//      allow to proceed only if the participant is in the study and the device is associated as welll
        if( verifyParticipant( studyId, participantId ) && verifyDevice( studyId, participantId, deviceId ) ){
        chronicleService.logData( studyId, participantId, deviceId, entitySetId, data );}
        else {
//            Throw an error?
        }
    }

    @Override
    @RequestMapping(
            path = STUDY_ID_PATH + PARTICIPANT_ID_PATH + DEVICE_ID_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public void enrollDevice( @PathVariable( STUDY_ID ) UUID studyId,
                              @PathVariable( PARTICIPANT_ID ) UUID participantId,
                              @PathVariable( DEVICE_ID ) String deviceId ) {
//      allow to proceed only if the participant is in the study and the device has not been associated yet
        if ( verifyParticipant( studyId, participantId ) && !verifyDevice( studyId, participantId, deviceId )) {
        chronicleService.enrollDevice( studyId, participantId, deviceId );}
        else {
//            Throw an error?
        }
    }

    @Override
    @RequestMapping(
            path = STUDY_ID_PATH + PARTICIPANT_ID_PATH + DEVICE_ID_PATH,
            method = RequestMethod.GET,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Boolean verifyDevice( @PathVariable( STUDY_ID ) UUID studyId,
                                 @PathVariable( PARTICIPANT_ID ) UUID participantId,
                                 @PathVariable( DEVICE_ID ) String deviceId ) {
//        validate that this device belongs to this participant in this study
//
    }

    @Override
    @RequestMapping(
            path = STUDY_ID_PATH + PARTICIPANT_ID_PATH,
            method = RequestMethod.GET,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Boolean verifyParticipant( @PathVariable( STUDY_ID ) UUID studyId,
                                      @PathVariable( PARTICIPANT_ID ) UUID participantId ) {
//        validate that this participant belongs in this study
    }
}
