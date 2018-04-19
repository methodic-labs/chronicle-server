package com.openlattice.chronicle.services;

import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.authorization.HazelcastAclKeyReservationService;

import javax.inject.Inject;
import java.util.UUID;
//  TODO: Need the information for the data parsing to do logData()

public class ChronicleService {

    @Inject
    private EventBus eventBus;

    public ChronicleService() { }

    public void logData( studyId, participantId ) {
//        TODO: Parse the data and integrate with datamodel
    }

    public void enrollDevice( studyId, participantId, deviceId ) {
//        TODO
    }

}