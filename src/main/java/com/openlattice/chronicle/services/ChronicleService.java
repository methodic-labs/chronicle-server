package com.openlattice.chronicle.services;

import com.openlattice.data.DataApi;
import com.google.common.collect.SetMultimap;
import com.google.common.eventbus.EventBus;
import com.openlattice.data.requests.BulkDataCreation;
import com.openlattice.data.requests.Association;

import javax.inject.Inject;
import java.util.UUID;
//  TODO: Need the information for the data parsing to do logData()

public class ChronicleService {

    @Inject
    private EventBus eventBus;

    public ChronicleService() { }

    public void logData(UUID studyId, UUID participantId, String deviceId, UUID entitySetId, SetMultimap<UUID, Object> data) {
//     create the syncTickets
//          create a fake syncId UUID
//              How is this done? can I use xxxxxxxx-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx
//          DataApi.acquireSyncTicket( entitySetId, fakeSyncId )

//     create the associations
//        var association = new Association( Key, srcKey, dstKey, data )
//              create one for each association and then combine into Set<associations>
//              EntityKey = ( entitySetId (uuid), syncId (uuid), entityId (string) )
//              Entity = ( EntityKey, data )
//          Key = ( entitySetId, fakeSyncId, ? )
//          srcKey = ( entitySetIdOfSrc, fakeSyncId, ? ) Do I use the same syncId?
//          dstKey = ( entitySetIdOfDst, fakeSyncId, ? ) Do I use the same syncId?

//     create the BulkData
//          var bulkData = new BulkDataCreation( Set<syncTickets>, Set<entities>, Set<associations> )
//     DataApi.createEntityAndAssociationData( BulkDataCreation bulkData );
    }

    public void enrollDevice( UUID studyId, UUID participantId, String deviceId ) {
//     add the device and associate to the participant and to the study
    }

}