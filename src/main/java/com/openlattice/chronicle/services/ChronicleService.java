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

    public ChronicleService() {
    }
//  TODO: add in throws exception!
    public void logData(
            UUID studyId,
            UUID participantId,
            String deviceId,
            UUID entitySetId,
            SetMultimap<UUID, Object> data ) throws Exception {
        //  TODO: Need the data model to exist before I can do anything here.

        //  TEMPLATE FOR BULK DATA CREATION
        //  var bulkData = new BulkDataCreation( Set<syncTickets>, Set<entities>, Set<associations> )
        //      Set<syncTickets> = empty set

        //      Set<entities> = ( entityKey, details )
        //          FOR EACH ENTITY ( for each: individual logs )
        //          entity = ( entityKey, details )
        //              entityKey = ( entitySetId (uuid), syncId (uuid), entityId (string) )
        //                  entitySetId = entitySetId --> PathVariable
        //                  syncId = SyncApi.getCurrentSyncId(entitySetId)
        //                  entityId = TODO this will be the logData entitySet
        //              data = data --> PathVariable

        //      Set<associations> = ( key, src, dst, details )
        //          FOR EACH ASSOCIATION ( for each: log --> Person, Study, Device )
        //          key = ( entitySetId (uuid), syncId (uuid), entityId (string) ) TODO this will be the association entitySet
        //              entitySetId = entitySetId --> PathVariable
        //              syncId = SyncApi.getCurrentSyncId(entitySetId)
        //              entityId = TODO
        //          src = ( entitySetId (uuid), syncId (uuid), entityId (string) )
        //              entitySetId = TODO
        //              syncId = SyncApi.getCurrentSyncId(TODO)
        //              entityId = TODO        //          dst =
        //          dst = ( entitySetId (uuid), syncId (uuid), entityId (string) )
        //              entitySetId = TODO
        //              syncId = SyncApi.getCurrentSyncId(TODO)
        //              entityId = TODO
        //          details = TODO put here what I want in the association set

        //

        //  DataApi.createEntityAndAssociationData( BulkDataCreation bulkData );
        //  TODO: Make sure to return any errors??? Currently void method.
    }

    public void enrollDevice( UUID studyId, UUID participantId, String deviceId ) throws Exception {
        //  previous logic already verified the participant and that the device is not already connected.
        //  add the device and associate to the participant and to the study
        //  this will be two associations device --> person, device --> study
        //  aka write a line to these association tables, createAssociationData() does not exist in lattice-js yet.
        //  DataApi.createEntityAndAssociationData() see example above for template
    }

}