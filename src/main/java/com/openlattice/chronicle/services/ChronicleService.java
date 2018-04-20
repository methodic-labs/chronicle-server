package com.openlattice.chronicle.services;

import com.openlattice.data.DataApi;
import com.google.common.collect.SetMultimap;
import com.google.common.eventbus.EventBus;
import com.openlattice.data.requests.BulkDataCreation;

import javax.inject.Inject;
import java.util.UUID;
//  TODO: Need the information for the data parsing to do logData()

public class ChronicleService {

    @Inject
    private EventBus eventBus;

    public ChronicleService() { }

    public void logData(UUID studyId, UUID participantId, String deviceId, UUID entitySetId, SetMultimap<UUID, Object> data) {
        DataApi.createEntityAndAssociationData( BulkDataCreation data );
    }

    public void enrollDevice( UUID studyId, UUID participantId, String deviceId ) {
//        verify the participant is enrolled in study ( verifyParticipant() --> true )
//        check to make sure the device does not already exist ( aka verifyDevice() --> false )
//        If false, then add the device and associate to the participant and to the study
    }

}