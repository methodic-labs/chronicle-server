package com.openlattice.chronicle.controllers.legacy;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.SetMultimap;
import com.openlattice.chronicle.ChronicleApi;
import com.openlattice.chronicle.services.edm.EdmCacheManager;
import com.openlattice.chronicle.services.upload.AppDataUploadManager;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@RestController
@RequestMapping( ChronicleApi.CONTROLLER )
public class ChronicleController implements ChronicleApi {

    @Inject
    private AppDataUploadManager dataUploadManager;

    @Inject
    private EdmCacheManager edmCacheManager;

    @Override
    @Timed
    @RequestMapping(
            path = STUDY_ID_PATH + PARTICIPANT_ID_PATH + DATASOURCE_ID_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE
    )
    public Integer upload(
            @PathVariable( STUDY_ID ) UUID studyId,
            @PathVariable( PARTICIPANT_ID ) String participantId,
            @PathVariable( DATASOURCE_ID ) String datasourceId,
            @RequestBody List<SetMultimap<UUID, Object>> data ) {

        return dataUploadManager.upload( null, studyId, participantId, datasourceId, data );
    }

    @Override
    @Timed
    @RequestMapping(
            path = EDM_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public Map<String, UUID> getPropertyTypeIds( @RequestBody Set<String> propertyTypeFqns ) {
        return edmCacheManager.getHistoricalPropertyTypeIds( propertyTypeFqns );
    }

    @Override
    @Timed
    @RequestMapping(
            path = STATUS_PATH,
            method = RequestMethod.GET
    )
    public Boolean isRunning() {
        //TODO: Ensure connectivity with OpenLattice backend.
        return true;
    }
}
