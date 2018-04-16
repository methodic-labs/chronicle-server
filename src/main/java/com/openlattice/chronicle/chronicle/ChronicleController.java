package com.openlattice.chronicle.chronicle;

import com.openlattice.chronicle.ChronicleApi;
import com.openlattice.chronicle.constants.CustomMediaType;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMethod;

import javax.inject.Inject;


@RestController
@RequestMapping( ChronicleApi.CONTROLLER )
public class ChronicleController implements ChronicleApi {

  @Inject
  private ChronicleService chronicleService;

  @RequestMapping(
          path = { "/" + STUDY_ID + "/" + PARTICIPANT_ID },
          method = RequestMethod.POST,
          // TODO
          produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE } ))
  // public methods here
  @Override
  // overridden public methods here
}
