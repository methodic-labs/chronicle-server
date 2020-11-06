package com.openlattice.chronicle.services;

import com.dataloom.streams.StreamUtil;
import com.google.common.collect.*;
import com.openlattice.chronicle.constants.*;
import com.openlattice.chronicle.services.edm.EdmCacheManager;
import com.openlattice.chronicle.services.entitysets.EntitySetIdsManager;
import com.openlattice.client.ApiClient;
import com.openlattice.data.DataApi;
import com.openlattice.data.requests.EntitySetSelection;
import com.openlattice.data.requests.FileType;
import com.openlattice.data.requests.NeighborEntityDetails;
import com.openlattice.directory.PrincipalApi;
import com.openlattice.entitysets.EntitySetsApi;
import com.openlattice.search.SearchApi;
import com.openlattice.search.requests.EntityNeighborsFilter;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.openlattice.chronicle.constants.AppComponent.CHRONICLE;
import static com.openlattice.chronicle.constants.AppComponent.CHRONICLE_DATA_COLLECTION;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.DEVICE;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.PARTICIPANTS;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.PARTICIPATED_IN;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.STUDIES;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.USER_APPS;
import static com.openlattice.chronicle.constants.EdmConstants.APPS_DICTIONARY_ES;
import static com.openlattice.chronicle.constants.EdmConstants.DEVICES_ES;
import static com.openlattice.chronicle.constants.EdmConstants.FULL_NAME_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.PARTICIPATED_IN_ES;
import static com.openlattice.chronicle.constants.EdmConstants.PERSON_ID_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.RECORDED_DATE_TIME_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.RECORD_TYPE_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.STRING_ID_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.STUDY_ES;
import static com.openlattice.chronicle.constants.EdmConstants.USER_APPS_ES;
import static com.openlattice.chronicle.util.ChronicleServerUtil.getFirstUUIDOrNull;
import static com.openlattice.chronicle.util.ChronicleServerUtil.getFirstValueOrNull;
import static com.openlattice.chronicle.util.ChronicleServerUtil.getParticipantEntitySetName;
import static com.openlattice.edm.EdmConstants.ID_FQN;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public class ScheduledTasksManager {
    protected final Logger logger = LoggerFactory.getLogger( ScheduledTasksManager.class );

    private final long USER_APPS_REFRESH_INTERVAL    = 60 * 1000; // 1 minute
    private final long STUDY_INFO_REFRESH_INTERVAL   = 60 * 1000; // 1 minute
    private final long SYNC_USER_REFRESH_INTERVAL    = 60 * 1000; // 1 minute
    private final long SYSTEM_APPS_REFRESH_INTERVAL  = 60 * 60 * 1000; // 1 hour
    private final long DEVICES_INFO_REFRESH_INTERVAL = 60 * 1000; // 1 minute

    // orgId -> studyId -> participantId -> EKID
    private final Map<UUID, Map<UUID, Map<String, UUID>>> studyParticipantsByOrg = Maps.newHashMap();

    // orgId -> studyId -> studyEKID
    private final Map<UUID, Map<UUID, UUID>> studyEntityKeyIdsByOrg = Maps.newHashMap();

    // orgId -> deviceId -> deviceEKID
    private final Map<UUID, Map<String, UUID>> deviceIdsByOrg = Maps.newHashMap();

    // app fullName -> { org1, org2, org3 }
    private final Map<String, Set<UUID>> userAppsFullNamesByOrg = Maps.newLinkedHashMap();

    // deviceId -> deviceEKID
    private final Map<String, UUID> deviceIdsByEKID = Maps.newHashMap();

    // studyId -> participantId -> participant EKID
    private final Map<UUID, Map<String, UUID>> studyParticipants = Maps.newHashMap();

    // studyId -> studyEKID
    private final Map<UUID, UUID> studyEKIDById = Maps.newHashMap();

    // app fullnames in chronicle_user_apps entity set
    private final Set<String> userAppsFullNameValues = Sets.newHashSet();

    // app fullnames in chronicle_application_dictionary entity set
    private final Set<String> systemAppPackageNames = Sets.newHashSet();

    private final ApiCacheManager     apiCacheManager;
    private final EdmCacheManager     edmCacheManager;
    private final EntitySetIdsManager entitySetIdsManager;

    public ScheduledTasksManager(
            ApiCacheManager apiCacheManager,
            EdmCacheManager edmCacheManager,
            EntitySetIdsManager entitySetIdsManager ) throws ExecutionException {
        this.apiCacheManager = apiCacheManager;
        this.edmCacheManager = edmCacheManager;
        this.entitySetIdsManager = entitySetIdsManager;

        syncCallingUser();
        refreshAllOrgsUserAppFullNames();
        refreshUserAppsFullNames();
        refreshAllOrgsStudyInformation();
        refreshSystemApps();
        refreshDevicesCache();
        refreshAllOrgsDevicesCache();
        refreshStudyInformation();
    }

    // helper methods
    private Iterable<SetMultimap<FullQualifiedName, Object>> getEntitySetData(
            DataApi dataApi,
            UUID entitySetId,
            Set<FullQualifiedName> fqns ) {
        return dataApi
                .loadSelectedEntitySetData(
                        entitySetId,
                        new EntitySetSelection(
                                Optional.of( fqns.stream().map( edmCacheManager::getPropertyTypeId ).collect(
                                        Collectors.toSet() ) )
                        ),
                        FileType.json
                );
    }

    @Scheduled( fixedRate = SYNC_USER_REFRESH_INTERVAL )
    public void syncCallingUser() {
        logger.info( "attempting to sync user " );
        try {
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            PrincipalApi principalApi = apiClient.getPrincipalApi();

            principalApi.syncCallingUser();
        } catch ( Exception e ) {
            logger.error( "error when attempting to sync user", e );
        }
    }

    @Scheduled( fixedRate = SYSTEM_APPS_REFRESH_INTERVAL )
    public void refreshSystemApps() {
        logger.info( "Refreshing system apps cache" );

        try {
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            DataApi dataApi = apiClient.getDataApi();

            Iterable<SetMultimap<FullQualifiedName, Object>> entitySetData = getEntitySetData(
                    dataApi,
                    entitySetIdsManager.getLegacyEntitySetId( APPS_DICTIONARY_ES ),
                    ImmutableSet.of( FULL_NAME_FQN, RECORD_TYPE_FQN )
            );
            logger.info(
                    "Fetched {} entities from chronicle_application_dictionary entity set",
                    Iterators.size( entitySetData.iterator() )
            );

            Set<String> systemAppPackageNames = Sets.newHashSet();

            entitySetData.forEach( entity -> {
                String packageName = getFirstValueOrNull( Multimaps.asMap( entity ), FULL_NAME_FQN );
                String recordType = getFirstValueOrNull( Multimaps.asMap( entity ), RECORDED_DATE_TIME_FQN );

                if ( RecordType.SYSTEM.name().equals( recordType ) && packageName != null ) {
                    systemAppPackageNames.add( packageName );
                }

            } );

            this.systemAppPackageNames.addAll( systemAppPackageNames );

            logger.info( "Loaded {} items into the system apps cache", systemAppPackageNames.size() );

        } catch ( Exception e ) {
            logger.error( "error when refreshing system apps cache", e );
        }
    }

    @Scheduled( fixedRate = USER_APPS_REFRESH_INTERVAL )
    public void refreshUserAppsFullNames() {
        logger.info( "refreshing user apps cache" );
        try {
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            DataApi dataApi = apiClient.getDataApi();

            // load entities from chronicle_user_apps
            Iterable<SetMultimap<FullQualifiedName, Object>> data = getEntitySetData(
                    dataApi,
                    entitySetIdsManager.getLegacyEntitySetId( USER_APPS_ES ),
                    ImmutableSet.of( FULL_NAME_FQN )
            );

            // get entity key ids
            Set<String> fullNames = StreamUtil.stream( data )
                    .map( entry -> getFirstValueOrNull( Multimaps.asMap( entry ), FULL_NAME_FQN ) )
                    .filter( Objects::nonNull )
                    .collect( Collectors.toSet() );

            userAppsFullNameValues.addAll( fullNames );

            logger.info( "loaded {} fullnames from chronicle_user_apps", fullNames.size() );
        } catch ( Exception e ) {
            logger.error( "error loading fullnames from chronicle_user_apps", e );
        }
    }

    @Scheduled( fixedRate = USER_APPS_REFRESH_INTERVAL )
    public void refreshAllOrgsUserAppFullNames() {
        logger.info( "refreshing all orgs user apps fullnames" );

        try {
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            DataApi dataApi = apiClient.getDataApi();

            Map<String, Set<UUID>> fullNamesMap = Maps.newLinkedHashMap(); // fullName -> { org1, org2, org3 }

            Map<UUID, Map<CollectionTemplateTypeName, UUID>> orgEntitySets = entitySetIdsManager
                    .getEntitySetIdsByOrgId()
                    .getOrDefault( CHRONICLE_DATA_COLLECTION, Map.of() );

            orgEntitySets.forEach( ( orgId, templateTypeESIDMap ) -> {
                // load entities from entity set
                Iterable<SetMultimap<FullQualifiedName, Object>> data = getEntitySetData(
                        dataApi,
                        templateTypeESIDMap.get( USER_APPS ),
                        ImmutableSet.of( FULL_NAME_FQN )
                );

                // map fullNames -> set of orgIds
                StreamUtil.stream( data )
                        .map( entity -> getFirstValueOrNull( Multimaps.asMap( entity ), FULL_NAME_FQN ) )
                        .filter( Objects::nonNull )
                        .forEach( fullName -> {
                            Set<UUID> organizations = fullNamesMap.getOrDefault( fullName, Sets.newHashSet() );
                            organizations.add( orgId );

                            fullNamesMap.put( fullName, organizations );
                        } );

            } );

            userAppsFullNamesByOrg.putAll( fullNamesMap );

            logger.info( "loaded {} fullnames from user apps entity sets in all orgs", fullNamesMap.keySet().size() );
        } catch ( Exception e ) {
            logger.info( "error loading all orgs user apps fullnames", e );
        }
    }

    @Scheduled( fixedRate = STUDY_INFO_REFRESH_INTERVAL )
    public void refreshAllOrgsStudyInformation() {
        logger.info( "refreshing study information for all organizations" );

        try {
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            DataApi dataApi = apiClient.getDataApi();
            SearchApi searchApi = apiClient.getSearchApi();

            Map<UUID, Map<UUID, Map<String, UUID>>> allParticipants = Maps
                    .newHashMap(); // orgId -> studyId -> participantId -> participantEKID
            Map<UUID, Map<UUID, UUID>> studyEntityKeyIds = Maps.newHashMap(); // orgId -> studyId -> studyEKID

            Map<UUID, Map<CollectionTemplateTypeName, UUID>> orgEntitySets = entitySetIdsManager
                    .getEntitySetIdsByOrgId()
                    .getOrDefault( CHRONICLE, Map.of() );

            orgEntitySets.forEach( ( orgId, templateTypeESIDMap ) -> {

                Map<UUID, UUID> studyIds = Maps
                        .newHashMap(); // studyEKID -> studyId (for lookup when processing neighbors)

                // entity set ids
                UUID participatedInESID = entitySetIdsManager.getEntitySetId( orgId, CHRONICLE, PARTICIPATED_IN );
                UUID participantsESID = entitySetIdsManager.getEntitySetId( orgId, CHRONICLE, PARTICIPANTS );
                UUID studiesESID = entitySetIdsManager.getEntitySetId( orgId, CHRONICLE, STUDIES );

                if ( studiesESID == null || participantsESID == null || participatedInESID == null )
                    return;

                // load studies entities
                Iterable<SetMultimap<FullQualifiedName, Object>> studyEntities = getEntitySetData(
                        dataApi,
                        studiesESID,
                        ImmutableSet.of( STRING_ID_FQN )
                );

                // map studyIds -> studyEKIDs
                StreamUtil.stream( studyEntities ).map( Multimaps::asMap ).forEach( ( entity ) -> {
                    UUID studyEKID = getFirstUUIDOrNull( entity, ID_FQN );
                    UUID studyId = getFirstUUIDOrNull( entity, STRING_ID_FQN );
                    if ( studyId == null )
                        return;

                    studyEntityKeyIds.computeIfAbsent( orgId, key -> Maps.newHashMap() ).put( studyId, studyEKID );
                    studyIds.put( studyEKID, studyId );
                } );

                if ( studyIds.isEmpty() ) {
                    logger.info( "study info refresh: organization {} does not have studies", orgId );
                    return;
                }

                // get study neighbors constrained by participated_in association
                Set<UUID> edgeESIDS = ImmutableSet.of( participatedInESID );
                Set<UUID> srcESIDS = ImmutableSet.of( participantsESID );

                Map<UUID, List<NeighborEntityDetails>> studyNeighbors = searchApi.executeFilteredEntityNeighborSearch(
                        studiesESID,
                        new EntityNeighborsFilter(
                                studyIds.keySet(),
                                Optional.of( srcESIDS ),
                                Optional.empty(),
                                Optional.of( edgeESIDS )
                        )
                );

                // process neighbors
                Map<UUID, Map<String, UUID>> participantsByStudy = Maps
                        .newHashMap(); // studyId -> participantId -> EKID
                studyNeighbors.forEach( ( studyEKID, neighbors ) -> {
                    UUID studyId = studyIds.get( studyEKID );

                    neighbors.forEach( neighbor -> {

                        // edge: participatedIn
                        String participantId = getFirstValueOrNull( neighbor.getNeighborDetails().get(),
                                PERSON_ID_FQN );
                        if ( participantId == null )
                            return;

                        UUID participantEKID = neighbor.getNeighborId().get();

                        participantsByStudy.computeIfAbsent( studyId, key -> Maps.newHashMap() )
                                .put( participantId, participantEKID );

                    } );
                } );

                allParticipants.put( orgId, participantsByStudy );

            } );

            this.studyParticipantsByOrg.putAll( allParticipants );
            this.studyEntityKeyIdsByOrg.putAll( studyEntityKeyIds );

            logger.info( "loaded {} study EKIDS from all orgs",
                    studyEntityKeyIdsByOrg.values().stream().mapToLong( map -> map.values().size() ).sum() );
            logger.info( "loaded {} participants from all orgs",
                    studyParticipantsByOrg.values().stream().flatMap( map -> map.values().stream() )
                            .mapToLong( map -> map.values().size() ).sum() );

        } catch ( Exception e ) {
            logger.error( "caught exception while refreshing study information for all orgs", e );
        }
    }

    @Scheduled( fixedRate = STUDY_INFO_REFRESH_INTERVAL )
    public void refreshStudyInformation() {
        logger.info( "refreshing study information for cafe studies" );

        try {
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            DataApi dataApi = apiClient.getDataApi();
            SearchApi searchApi = apiClient.getSearchApi();
            EntitySetsApi entitySetsApi = apiClient.getEntitySetsApi();

            Map<UUID, UUID> studyEKIDById = Maps.newHashMap(); // studyId -> studyEKID

            Map<UUID, Map<String, UUID>> participants = Maps
                    .newHashMap(); // studyID -> participantId -> participantEKID

            // entity set ids
            UUID participatedInESID = entitySetIdsManager.getLegacyEntitySetId( PARTICIPATED_IN_ES );
            UUID studiesESID = entitySetIdsManager.getLegacyEntitySetId( STUDY_ES );

            // get study entities
            Iterable<SetMultimap<FullQualifiedName, Object>> studyEntities = getEntitySetData(
                    dataApi,
                    studiesESID,
                    ImmutableSet.of( STRING_ID_FQN )
            );

            // process studies
            StreamUtil.stream( studyEntities ).map( Multimaps::asMap ).forEach( ( entity ) -> {
                UUID studyEKID = getFirstUUIDOrNull( entity, ID_FQN );
                UUID studyId = getFirstUUIDOrNull( entity, STRING_ID_FQN );
                if ( studyId == null )
                    return;

                studyEKIDById.put( studyId, studyEKID );
            } );

            studyEKIDById.keySet().forEach( studyId -> {
                String participantES = getParticipantEntitySetName( studyId );
                UUID participantESID = entitySetsApi.getEntitySetId( participantES );

                Map<UUID, List<NeighborEntityDetails>> studyNeighbors = searchApi.executeFilteredEntityNeighborSearch(
                        studiesESID,
                        new EntityNeighborsFilter(
                                new HashSet<>( studyEKIDById.values() ),
                                Optional.of( ImmutableSet.of( participantESID ) ),
                                Optional.empty(),
                                Optional.of( ImmutableSet.of( participatedInESID ) )
                        )
                );

                studyNeighbors.forEach( ( studyEKID, neighbors ) -> {
                    neighbors.forEach( neighbor -> {
                        String participantId = getFirstValueOrNull( neighbor.getNeighborDetails().get(),
                                PERSON_ID_FQN );
                        if ( participantId == null )
                            return;

                        UUID participantEKID = neighbor.getNeighborId().get();

                        participants.computeIfAbsent( studyId, id -> Maps.newHashMap() )
                                .put( participantId, participantEKID );
                    } );

                } );
            } );

            this.studyEKIDById.putAll( studyEKIDById );
            this.studyParticipants.putAll( participants );

            logger.info( "loaded {} study EKIDs from cafe studies", studyEKIDById.size() );
            logger.info( "loaded {} participants from cafe participants",
                    participants.values().stream().mapToInt( map -> map.values().size() ).sum() );

        } catch ( Exception e ) {
            logger.error( "error refreshing study information for cafe studies", e );
        }
    }

    @Scheduled( fixedRate = DEVICES_INFO_REFRESH_INTERVAL )
    public void refreshAllOrgsDevicesCache() {
        logger.info( "refreshing devices info for all orgs" );

        try {
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            DataApi dataApi = apiClient.getDataApi();

            Map<UUID, Map<CollectionTemplateTypeName, UUID>> orgEntitySets = entitySetIdsManager
                    .getEntitySetIdsByOrgId()
                    .getOrDefault( CHRONICLE_DATA_COLLECTION, Map.of() );

            Map<UUID, Map<String, UUID>> deviceIdsByOrg = Maps.newHashMap();
            orgEntitySets.forEach( ( orgId, templateTypeESIDMap ) -> {
                Map<String, UUID> devicesByEKID = getDeviceIdsByEKID(
                        dataApi,
                        templateTypeESIDMap.get( DEVICE )
                );
                deviceIdsByOrg.put( orgId, devicesByEKID );
            } );

            this.deviceIdsByOrg.putAll( deviceIdsByOrg );

            logger.info( "loaded {} deviceIds from all orgs",
                    deviceIdsByOrg.values().stream().mapToInt( map -> map.values().size() ).sum() );

        } catch ( Exception e ) {
            logger.error( "error refreshing devices info for all orgs" );
        }
    }

    @Scheduled( fixedRate = DEVICES_INFO_REFRESH_INTERVAL )
    public void refreshDevicesCache() {
        logger.info( "refreshing devices info for cafe users" );
        try {
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            DataApi dataApi = apiClient.getDataApi();

            Map<String, UUID> deviceIdsByEKID = getDeviceIdsByEKID( dataApi,
                    entitySetIdsManager.getLegacyEntitySetId( DEVICES_ES ) );

            this.deviceIdsByEKID.putAll( deviceIdsByEKID );

            logger.info( "loaded {} deviceIds for cafe users", deviceIdsByEKID.size() );

        } catch ( Exception e ) {
            logger.error( "error refreshing devices cache for cafe users", e );
        }
    }

    private Map<String, UUID> getDeviceIdsByEKID( DataApi dataApi, UUID entitySetId ) {
        Map<String, UUID> deviceIds = Maps.newHashMap();

        // load entities
        Iterable<SetMultimap<FullQualifiedName, Object>> deviceEntities = getEntitySetData(
                dataApi,
                entitySetId,
                ImmutableSet.of( STRING_ID_FQN )
        );

        StreamUtil.stream( deviceEntities ).map( Multimaps::asMap ).forEach( ( entity ) -> {
            UUID deviceEKID = getFirstUUIDOrNull( entity, ID_FQN );
            String deviceId = getFirstValueOrNull( entity, STRING_ID_FQN );
            if ( deviceId == null )
                return;

            deviceIds.put( deviceId, deviceEKID );
        } );

        return deviceIds;
    }

    public Map<UUID, Map<UUID, Map<String, UUID>>> getStudyParticipantsByOrg() {
        return studyParticipantsByOrg;
    }

    public Map<UUID, Map<UUID, UUID>> getStudyEntityKeyIdsByOrg() {
        return studyEntityKeyIdsByOrg;
    }

    public Map<UUID, Map<String, UUID>> getDeviceIdsByOrg() {
        return deviceIdsByOrg;
    }

    public Map<String, Set<UUID>> getUserAppsFullNamesByOrg() {
        return userAppsFullNamesByOrg;
    }

    public Map<String, UUID> getDeviceIdsByEKID() {
        return deviceIdsByEKID;
    }

    public Map<UUID, Map<String, UUID>> getStudyParticipants() {
        return studyParticipants;
    }

    public Map<UUID, UUID> getStudyEKIDById() {
        return studyEKIDById;
    }

    public Set<String> getUserAppsFullNameValues() {
        return userAppsFullNameValues;
    }

    public Set<String> getSystemAppPackageNames() {
        return systemAppPackageNames;
    }
}
