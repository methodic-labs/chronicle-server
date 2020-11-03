package com.openlattice.chronicle.services;

import com.dataloom.streams.StreamUtil;
import com.google.common.collect.*;
import com.openlattice.apps.App;
import com.openlattice.apps.AppApi;
import com.openlattice.apps.UserAppConfig;
import com.openlattice.authorization.securable.AbstractSecurableObject;
import com.openlattice.chronicle.constants.*;
import com.openlattice.client.ApiClient;
import com.openlattice.collections.CollectionTemplateType;
import com.openlattice.collections.CollectionsApi;
import com.openlattice.collections.EntitySetCollection;
import com.openlattice.collections.EntityTypeCollection;
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

    private final long ENTITY_SETS_REFRESH_INTERVAL  = 15 * 60 * 1000; // 15 minutes
    private final long USER_APPS_REFRESH_INTERVAL    = 60 * 1000; // 1 minute
    private final long STUDY_INFO_REFRESH_INTERVAL   = 60 * 1000; // 1 minute
    private final long SYNC_USER_REFRESH_INTERVAL    = 60 * 1000; // 1 minute
    private final long SYSTEM_APPS_REFRESH_INTERVAL  = 60 * 60 * 1000; // 1 hour
    private final long DEVICES_INFO_REFRESH_INTERVAL = 60 * 1000; // 1 minute

    public Map<AppComponent, Map<UUID, Map<CollectionTemplateTypeName, UUID>>> getEntitySetIdsByOrgId() {
        return entitySetIdsByOrgId;
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

    // appName -> orgId -> templateName -> entitySetID
    private final Map<AppComponent, Map<UUID, Map<CollectionTemplateTypeName, UUID>>> entitySetIdsByOrgId = Maps
            .newHashMap();

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

    private final ApiCacheManager apiCacheManager;
    private final EdmCacheManager edmCacheManager;

    public ScheduledTasksManager(
            ApiCacheManager apiCacheManager,
            EdmCacheManager edmCacheManager ) throws ExecutionException {
        this.apiCacheManager = apiCacheManager;
        this.edmCacheManager = edmCacheManager;

        syncCallingUser();
        initializeEntitySets();
        refreshAllOrgsUserAppFullNames();
        refreshUserAppsFullNames();
        refreshAllOrgsStudyInformation();
        refreshSystemApps();
        refreshDevicesCache();
        refreshAllOrgsDevicesCache();
        refreshStudyInformation();
    }

    @Scheduled( fixedRate = SYNC_USER_REFRESH_INTERVAL )
    public void syncCallingUser() {
        logger.info( "attempting to sync user " );
        try {
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            PrincipalApi principalApi = apiClient.getPrincipalApi();

            principalApi.syncCallingUser();
        } catch ( Exception e ) {
            logger.info( "error when attempting to sync user" );
        }
    }

    @Scheduled( fixedRate = ENTITY_SETS_REFRESH_INTERVAL )
    public void initializeEntitySets() throws ExecutionException {
        logger.info( "Refreshing entity set ids map" );

        ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
        AppApi appApi = apiClient.getAppApi();
        CollectionsApi collectionsApi = apiClient.getCollectionsApi();

        // create a app -> appId mapping
        Map<AppComponent, UUID> appNameIdMap = Maps.newHashMapWithExpectedSize( AppComponent.values().length );
        for ( AppComponent component : AppComponent.values() ) {
            App app = appApi.getAppByName( component.toString() );
            appNameIdMap.put( component, app.getId() );
        }

        Map<AppComponent, Map<UUID, Map<CollectionTemplateTypeName, UUID>>> entitySets = new HashMap<>();

        // get configs for each app
        appNameIdMap.forEach( ( appComponent, appId ) -> {
            List<UserAppConfig> configs = appApi.getAvailableAppConfigs( appId );

            if ( configs.isEmpty() )
                return;

            // get EntityTypeCollection associated with app
            EntitySetCollection entitySetCollection = collectionsApi
                    .getEntitySetCollection( configs.get( 0 ).getEntitySetCollectionId() );
            EntityTypeCollection entityTypeCollection = collectionsApi
                    .getEntityTypeCollection( entitySetCollection.getEntityTypeCollectionId() );

            // create mapping from templateTypeName -> templateTypeId
            Map<String, UUID> templateTypeNameIdMap = entityTypeCollection
                    .getTemplate()
                    .stream()
                    .collect( Collectors.toMap( CollectionTemplateType::getName, AbstractSecurableObject::getId ) );

            // for each config map orgId -> templateTypeName -> ESID
            Map<UUID, Map<CollectionTemplateTypeName, UUID>> orgEntitySetMap = new HashMap<>();

            configs.forEach( userAppConfig -> {
                Map<UUID, UUID> templateTypeIdESIDMap = collectionsApi
                        .getEntitySetCollection( userAppConfig.getEntitySetCollectionId() ).getTemplate();

                // iterate over templateTypeName enums and create mapping templateTypeName -> entitySetId
                Map<CollectionTemplateTypeName, UUID> templateTypeNameESIDMap = Maps
                        .newHashMapWithExpectedSize( templateTypeIdESIDMap.size() );

                for ( CollectionTemplateTypeName templateTypeName : CollectionTemplateTypeName.values() ) {
                    UUID templateTypeId = templateTypeNameIdMap.get( templateTypeName.toString() );

                    if ( templateTypeId == null )
                        continue;

                    templateTypeNameESIDMap.put( templateTypeName, templateTypeIdESIDMap.get( templateTypeId ) );
                }
                orgEntitySetMap.put( userAppConfig.getOrganizationId(), templateTypeNameESIDMap );
            } );

            entitySets.put( appComponent, orgEntitySetMap );
        } );

        entitySetIdsByOrgId.putAll( entitySets );
    }

    @Scheduled( fixedRate = SYSTEM_APPS_REFRESH_INTERVAL )
    public void refreshSystemApps() {
        logger.info( "Refreshing system apps cache" );

        try {
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            DataApi dataApi = apiClient.getDataApi();

            UUID fullNamePTID = edmCacheManager.getPropertyTypeIdsByFQN().get( FULL_NAME_FQN );
            UUID recordTypePTID = edmCacheManager.getPropertyTypeIdsByFQN().get( RECORD_TYPE_FQN );

            Iterable<SetMultimap<FullQualifiedName, Object>> entitySetData = dataApi.loadSelectedEntitySetData(
                    edmCacheManager.getEntitySetIdMap().get( APPS_DICTIONARY_ES ),
                    new EntitySetSelection( Optional.of( ImmutableSet.of( fullNamePTID, recordTypePTID ) ) ),
                    FileType.json
            );
            logger.info(
                    "Fetched {} entities from chronicle_application_dictionary entity set",
                    Iterators.size( entitySetData.iterator() )
            );

            Set<String> systemAppPackageNames = Sets.newHashSet();

            entitySetData.forEach( entity -> {
                String packageName = null;

                if ( !entity.get( FULL_NAME_FQN ).isEmpty() ) {
                    packageName = entity.get( FULL_NAME_FQN ).iterator().next().toString();
                }

                String recordType = null;
                if ( !entity.get( RECORD_TYPE_FQN ).isEmpty() ) {
                    recordType = entity.get( RECORD_TYPE_FQN ).iterator().next().toString();
                }

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

            UUID fullNamePTID = edmCacheManager.getPropertyTypeIdsByFQN().get( FULL_NAME_FQN );

            // load entities from chronicle_user_apps
            Iterable<SetMultimap<FullQualifiedName, Object>> data = dataApi
                    .loadSelectedEntitySetData(
                            edmCacheManager.getEntitySetIdMap().get( USER_APPS_ES ),
                            new EntitySetSelection(
                                    Optional.of( Set.of( fullNamePTID ) )
                            ),
                            FileType.json
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

            Map<UUID, Map<CollectionTemplateTypeName, UUID>> orgEntitySets = entitySetIdsByOrgId
                    .getOrDefault( CHRONICLE_DATA_COLLECTION, Map.of() );

            orgEntitySets.forEach( ( orgId, templateTypeESIDMap ) -> {
                // load entities from entity set
                Iterable<SetMultimap<FullQualifiedName, Object>> data = dataApi
                        .loadSelectedEntitySetData(
                                templateTypeESIDMap.get( USER_APPS ),
                                new EntitySetSelection(
                                        Optional.of( ImmutableSet
                                                .of( edmCacheManager.getPropertyTypeIdsByFQN().get( FULL_NAME_FQN ) ) )
                                ),
                                FileType.json
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

            Map<UUID, Map<CollectionTemplateTypeName, UUID>> orgEntitySets = entitySetIdsByOrgId
                    .getOrDefault( CHRONICLE, Map.of() );

            orgEntitySets.forEach( ( orgId, templateTypeESIDMap ) -> {

                Map<UUID, UUID> studyIds = Maps
                        .newHashMap(); // studyEKID -> studyId (for lookup when processing neighbors)

                // entity set ids
                UUID studiesESID = entitySetIdsByOrgId.getOrDefault( CHRONICLE, Map.of() )
                        .getOrDefault( orgId, Map.of() ).getOrDefault( STUDIES, null );

                UUID participantsESID = entitySetIdsByOrgId.getOrDefault( CHRONICLE, Map.of() )
                        .getOrDefault( orgId, Map.of() ).getOrDefault( PARTICIPANTS, null );
                UUID participatedInESID = entitySetIdsByOrgId.getOrDefault( CHRONICLE, Map.of() )
                        .getOrDefault( orgId, Map.of() ).getOrDefault( PARTICIPATED_IN, null );

                if ( studiesESID == null || participantsESID == null )
                    return;

                Iterable<SetMultimap<FullQualifiedName, Object>> studyEntities = dataApi
                        .loadSelectedEntitySetData(
                                studiesESID,
                                new EntitySetSelection(
                                        Optional.of( ImmutableSet.of(
                                                edmCacheManager.getPropertyTypeIdsByFQN().get( STRING_ID_FQN )
                                        ) )
                                ),
                                FileType.json
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

                // get study neighbors constrained by used_by and participated_in associations
                Set<UUID> edgeESIDS = Sets.newHashSet( participatedInESID )
                        .stream().filter( Objects::nonNull )
                        .collect( Collectors.toSet() );

                Set<UUID> srcESIDS = Sets.newHashSet( participantsESID )
                        .stream().filter( Objects::nonNull )
                        .collect( Collectors.toSet() );

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
            Map<UUID, UUID> studyIdByEKID = Maps.newHashMap(); // studyEKID -> studyId

            Map<UUID, Map<String, UUID>> participants = Maps
                    .newHashMap(); // studyID -> participantId -> participantEKID

            // entity set ids
            UUID participatedInESID = edmCacheManager.getEntitySetIdMap().get( PARTICIPATED_IN_ES );
            UUID studiesESID = edmCacheManager.getEntitySetIdMap().get( STUDY_ES );

            Iterable<SetMultimap<FullQualifiedName, Object>> studyEntities = dataApi
                    .loadSelectedEntitySetData(
                            studiesESID,
                            new EntitySetSelection(
                                    Optional.of( ImmutableSet.of(
                                            edmCacheManager.getPropertyTypeIdsByFQN().get( STRING_ID_FQN )
                                    ) )
                            ),
                            FileType.json
                    );

            // process studies
            StreamUtil.stream( studyEntities ).map( Multimaps::asMap ).forEach( ( entity ) -> {
                UUID studyEKID = getFirstUUIDOrNull( entity, ID_FQN );
                UUID studyId = getFirstUUIDOrNull( entity, STRING_ID_FQN );
                if ( studyId == null )
                    return;

                studyEKIDById.put( studyId, studyEKID );
                studyIdByEKID.put( studyEKID, studyId );
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

            Map<UUID, Map<CollectionTemplateTypeName, UUID>> orgEntitySets = entitySetIdsByOrgId
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
                    edmCacheManager.getEntitySetIdMap().get( DEVICES_ES ) );

            this.deviceIdsByEKID.putAll( deviceIdsByEKID );

            logger.info( "loaded {} deviceIds for cafe users", deviceIdsByEKID.size() );

        } catch ( Exception e ) {
            logger.error( "error refreshing devices cache for cafe users", e );
        }
    }

    private Map<String, UUID> getDeviceIdsByEKID( DataApi dataApi, UUID entitySetId ) {
        Map<String, UUID> deviceIds = Maps.newHashMap();

        // load entities
        Iterable<SetMultimap<FullQualifiedName, Object>> deviceEntities = dataApi
                .loadSelectedEntitySetData(
                        entitySetId,
                        new EntitySetSelection(
                                Optional.of( ImmutableSet.of(
                                        edmCacheManager.getPropertyTypeIdsByFQN().get( STRING_ID_FQN )
                                ) )
                        ),
                        FileType.json
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
}
