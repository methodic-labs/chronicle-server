package com.openlattice.chronicle.services;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.eventbus.EventBus;
import com.openlattice.data.DataApi;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.scheduling.annotation.Scheduled;

public class ChronicleServiceImpl implements ChronicleService {
    private final Map<UUID, SetMultimap<UUID, String>> studyInformation = new HashMap<>();

    private final EventBus eventBus;
    private final DataApi  dataApi;

    public ChronicleServiceImpl( EventBus evenutBus, DataApi dataApi ) {
        this.eventBus = evenutBus;
        this.dataApi = dataApi;
        refreshStudyInformation();
    }

    //  TODO: add in throws exception!
    @Override
    public Integer logData(
            UUID studyId,
            UUID participantId,
            String deviceId,
            List<SetMultimap<UUID, Object>> data ) {
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
        //  TODO:s Make sure to return any errors??? Currently void method.
        return data.size();
    }

    @Override public UUID registerDatasource( UUID studyId, UUID participantId, String datasourceId ) {

        //  previous logic already verified the participant and that the device is not already connected.
        //  add the device and associate to the participant and to the study
        //  this will be two associations device --> person, device --> study
        //  aka write a line to these association tables, createAssociationData() does not exist in lattice-js yet.
        //  DataApi.createEntityAndAssociationData() see example above for template
        studyInformation.computeIfAbsent( studyId, key -> HashMultimap.create() ).put( participantId, datasourceId );

        //TODO: Return entity key id for datasource instead of random uuid.
        //TODO: Write to openlattice
        return UUID.randomUUID();
    }

    @Override public boolean isKnownDatasource( UUID studyId, UUID participantId, String datasourceId ) {
        SetMultimap<UUID, String> participantDevices = Preconditions
                .checkNotNull( studyInformation.get( studyId ), "Study must exist." );

        return participantDevices.get( participantId ).contains( datasourceId );
    }

    @Override public boolean isKnownParticipant( UUID studyId, UUID participantId ) {
        SetMultimap<UUID, String> participantDevices = Preconditions
                .checkNotNull( studyInformation.get( studyId ), "Study must exist." );

        return participantDevices.containsKey( participantId );
    }

    @Scheduled( fixedRate = 60000 )
    public void refreshStudyInformation() {
        //TODO: Pull study information from prod
    }

}