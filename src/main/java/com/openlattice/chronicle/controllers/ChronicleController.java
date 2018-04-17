package com.openlattice.chronicle.controllers;

import com.openlattice.chronicle.services.ChronicleService;
import com.openlattice.chronicle.ChronicleApi;
import com.openlattice.chronicle.constants.CustomMediaType;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestBody;

import javax.inject.Inject;
import java.util.UUID;
import java.util.Set;

// TODO: what to put in the producers = {} part???
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
            @PathVariable( PARTICIPANT_ID ) UUID participantId) {
//       TODO: Do something with the data?
    }

    @Override
    @RequestMapping(
            path = STUDY_ID + PARTICIPANT_ID + DEVICE_ID_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public void enrollDevice( @PathVariable( STUDY_ID) UUID studyId,
                              @PathVariable( PARTICIPANT_ID ) UUID participantId,
                              @PathVariable( DEVICE_ID ) String deviceId); {}

    @Override
    @RequestMapping(
            path = STUDY_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
    public void createStudy( @RequestBody Study study) {}

    @Override
    @RequestMapping(
            path = STUDY_ID_PATH,
            method = RequestMethod.DELETE,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public void deleteStudy( @PathVariable( STUDY_ID ) UUID studyId) {}

    @Override
    @RequestMapping(
            path = STUDY_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<Study> getAllStudies() {}

    @Override
    @RequestMapping(
            path = STUDY_PATH + STUDY_ID_PATH,
            method = RequestMethod.GET,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Study getStudyById( @PathVariable( STUDY_ID ) UUID studyId) {}

    @Override
    @RequestMapping(
            path = PARTICIPANT_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Study createParticipant( @RequestBody Person participant) {}

    @Override
    @RequestMapping(
            path = PARTICIPANT_ID_PATH,
            method = RequestMethod.DELETE,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Study deleteParticipant( @PathVariable( PARTICIPANT_ID ) UUID participantId) {}

    @Override
    @RequestMapping(
            path = PARTICIPANT_PATH,
            method = RequestMethod.GET,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<Person> getAllParticipants() {}

    @Override
    @RequestMapping(
            path = STUDY_ID_PATH + PARTICIPANT_PATH,
            method = RequestMethod.GET,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<Person> getParticipantsFromStudy( @PathVariable( STUDY_ID ) UUID studyId) {}

    @Override
    @RequestMapping(
            path = PARTICIPANT_ID_PATH,
            method = RequestMethod.GET,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Person getParticipantById( @PathVariable( PARTICIPANT_ID ) UUID participantId) {}

    @Override
    @RequestMapping(
            path = UPDATE_PATH + PARTICIPANT_ID_PATH,
            method = RequestMethod.PATCH,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public void updateParticipantMetadata( @PathVariable( PARTICIPANT_ID ) UUID participantId,
                                           @RequestBody MetadataUpdate metadataupdate) {}

    @Override
    @RequestMapping(
            path = UPDATE_PATH + STUDY_ID_PATH,
            method = RequestMethod.PATCH,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public void updateStudyMetadata( @PathVariable( STUDY_ID ) UUID studyId,
                                     @RequestBody MetadataUpdate metadataupdate){}

    @Override
    @RequestMapping(
            path = INSTALL_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public void addParticipantToStudy( @PathVariable( STUDY_ID )UUID studyID,
                                       @PathVariable( PARTICIPANT_ID ) UUID participantId) {}

    @Override
    @RequestMapping(
            path = INSTALL_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH,
            method = RequestMethod.DELETE,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public void removeParticipantFromStudy( @PathVariable( STUDY_ID )UUID studyID,
                                              @PathVariable( PARTICIPANT_ID ) UUID participantId) {}

    @Override
    @RequestMapping(
            path = INSTALL_PATH + STUDY_ID_PATH + BULK_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public void addParticipantsToStudy( @PathVariable( STUDY_ID )UUID studyID,
                                       @PathVariable( PARTICIPANT_ID ) UUID participantId) {}
    @Override
    @RequestMapping(
            path = INSTALL_PATH + STUDY_ID_PATH + BULK_PATH,
            method = RequestMethod.DELETE,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public void removeParticipantsFromStudy( @PathVariable( STUDY_ID )UUID studyID,
                                              @PathVariable( PARTICIPANT_ID ) UUID participantId) {}
}
