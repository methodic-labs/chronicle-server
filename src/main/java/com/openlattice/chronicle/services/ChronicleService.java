package com.openlattice.chronicle.services;

import com.google.common.collect.SetMultimap;
import com.google.common.eventbus.EventBus;

import javax.inject.Inject;
import java.util.UUID;
//  TODO: Need the information for the data parsing to do logData()

public class ChronicleService {

    @Inject
    private EventBus eventBus;

    public ChronicleService() { }

    public void logData(UUID studyId, UUID participantId, SetMultimap<UUID, Object> data) {
//        TODO: Parse the data and integrate with datamodel
    }

    public void enrollDevice( UUID studyId, UUID participantId, String deviceId ) {
//        TODO
    }

}