package com.openlattice.chronicle.controllers;

import com.openlattice.chronicle.services.ChronicleService;
import com.openlattice.chronicle.ChronicleApi;
import com.openlattice.chronicle.constants.CustomMediaType;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMethod;

import javax.inject.Inject;


@RestController
@RequestMapping( ChronicleApi.CONTROLLER )
public class ChronicleController implements ChronicleApi {

    @Inject
    private ChronicleService chronicleService;

    @Override
    @RequestMapping(
            path = { "/" + STUDY_ID + "/" + PARTICIPANT_ID },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE } )
    public void logData(
            @PathVariable( STUDY_ID ) String studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId) {
//            Do something with the data?
    }

}
