package com.openlattice.chronicle.services;

import com.dataloom.streams.StreamUtil;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.*;
import com.google.common.eventbus.EventBus;
import com.openlattice.ApiUtil;
import com.openlattice.chronicle.ChronicleServerUtil;
import com.openlattice.chronicle.configuration.ChronicleConfiguration;
import com.openlattice.chronicle.sources.AndroidDevice;
import com.openlattice.chronicle.sources.Datasource;
import com.openlattice.chronicle.sources.EntitySetData;
import com.openlattice.client.ApiClient;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.data.DataApi;
import com.openlattice.data.DataIntegrationApi;
import com.openlattice.data.EntityKey;
import com.openlattice.data.integration.Association;
import com.openlattice.data.integration.BulkDataCreation;
import com.openlattice.data.integration.Entity;
import com.openlattice.data.requests.NeighborEntityDetails;
import com.openlattice.edm.EdmApi;
import com.openlattice.search.SearchApi;
import com.openlattice.search.requests.DataSearchResult;
import com.openlattice.search.requests.SearchTerm;
import com.openlattice.shuttle.MissionControl;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ChronicleServiceImpl implements ChronicleService {
    protected static final Logger            logger          = LoggerFactory.getLogger( ChronicleServiceImpl.class );
    private static final   FullQualifiedName INTERNAL_ID_FQN = new FullQualifiedName( "openlattice.@id" );

    private final Map<UUID, SetMultimap<String, String>> studyInformation  = new HashMap<>();
    private final SetMultimap<UUID, String>              studyParticipants = HashMultimap.create();

    private final String username;
    private final String password;

    private final EventBus          eventBus;
    private final String            STUDY_ENTITY_SET_NAME       = "chronicle_study";
    private final String            DEVICES_ENTITY_SET_NAME     = "chronicle_device";
    private final String            DATA_ENTITY_SET_NAME        = "chronicle_app_data";
    private final String            RECORDED_BY_ENTITY_SET_NAME = "chronicle_recorded_by";
    private final String            USED_BY_ENTITY_SET_NAME     = "chronicle_used_by";
    private final Set<UUID>         dataKey;
    private final FullQualifiedName STRING_ID_FQN               = new FullQualifiedName( "general.stringid" );
    private final FullQualifiedName PERSON_ID_FQN               = new FullQualifiedName( "nc.SubjectIdentification" );
    private final FullQualifiedName DATE_LOGGED_FQN             = new FullQualifiedName( "ol.datelogged" );
    private final FullQualifiedName VERSION_FQN                 = new FullQualifiedName( "ol.version" );
    private final FullQualifiedName MODEL_FQN                   = new FullQualifiedName( "vehicle.model" );
    private final UUID              studyEntitySetId;
    private final UUID              deviceEntitySetId;
    private final UUID              dataEntitySetId;
    private final UUID              recordedByEntitySetId;
    private final UUID              usedByEntitySetId;
    private final UUID              stringIdPropertyTypeId;
    private final UUID              participantIdPropertyTypeId;
    private final UUID              dateLoggedPropertyTypeId;
    private final UUID              versionPropertyTypeId;
    private final UUID              modelPropertyTypeId;

    private transient LoadingCache<Class<?>, ApiClient> apiClientCache = null;

    public ChronicleServiceImpl(
            EventBus eventBus,
            ChronicleConfiguration chronicleConfiguration ) throws ExecutionException {
        this.eventBus = eventBus;
        this.username = chronicleConfiguration.getUser();
        this.password = chronicleConfiguration.getPassword();

        apiClientCache = CacheBuilder
                .newBuilder()
                .expireAfterWrite( 10, TimeUnit.HOURS )
                .build( new CacheLoader<Class<?>, ApiClient>() {
                    @Override
                    public ApiClient load( Class<?> key ) throws Exception {
                        String jwtToken = MissionControl.getIdToken( username, password );
                        return new ApiClient( () -> jwtToken );
                    }
                } );

        ApiClient apiClient = apiClientCache.get( ApiClient.class );

        EdmApi edmApi = apiClient.getEdmApi();

        studyEntitySetId = edmApi.getEntitySetId( STUDY_ENTITY_SET_NAME );
        deviceEntitySetId = edmApi.getEntitySetId( DEVICES_ENTITY_SET_NAME );
        dataEntitySetId = edmApi.getEntitySetId( DATA_ENTITY_SET_NAME );
        recordedByEntitySetId = edmApi.getEntitySetId( RECORDED_BY_ENTITY_SET_NAME );
        usedByEntitySetId = edmApi.getEntitySetId( USED_BY_ENTITY_SET_NAME );

        stringIdPropertyTypeId = edmApi.getPropertyTypeId( STRING_ID_FQN.getNamespace(), STRING_ID_FQN.getName() );
        participantIdPropertyTypeId = edmApi.getPropertyTypeId( PERSON_ID_FQN.getNamespace(), PERSON_ID_FQN.getName() );
        dateLoggedPropertyTypeId = edmApi
                .getPropertyTypeId( DATE_LOGGED_FQN.getNamespace(), DATE_LOGGED_FQN.getName() );
        versionPropertyTypeId = edmApi.getPropertyTypeId( VERSION_FQN.getNamespace(), VERSION_FQN.getName() );
        modelPropertyTypeId = edmApi.getPropertyTypeId( MODEL_FQN.getNamespace(), MODEL_FQN.getName() );

        dataKey = edmApi.getEntityType( edmApi.getEntitySet( dataEntitySetId ).getEntityTypeId() )
                .getKey();

        refreshStudyInformation();

    }

    private Entity getDeviceEntity( String deviceId, Optional<Datasource> datasource ) {
        SetMultimap<UUID, Object> deviceData = HashMultimap.create();
        deviceData.put( stringIdPropertyTypeId, deviceId );
        if ( datasource.isPresent() && AndroidDevice.class.isAssignableFrom( datasource.get().getClass() ) ) {
            AndroidDevice device = (AndroidDevice) datasource.get();
            deviceData.put( modelPropertyTypeId, device.getModel() );
            deviceData.put( versionPropertyTypeId, device.getOsVersion() );
        }
        EntityKey deviceEntityKey = new EntityKey( deviceEntitySetId,
                ApiUtil.generateDefaultEntityId( ImmutableList.of( stringIdPropertyTypeId ), deviceData ) );
        return new Entity( deviceEntityKey, deviceData );
    }

    private Entity getStudyEntity( UUID studyId ) {
        SetMultimap<UUID, Object> studyData = HashMultimap.create();
        studyData.put( stringIdPropertyTypeId, studyId.toString() );
        EntityKey studyEntityKey = new EntityKey( studyEntitySetId,
                ApiUtil.generateDefaultEntityId( ImmutableList.of( stringIdPropertyTypeId ), studyData ) );
        return new Entity( studyEntityKey, studyData );
    }

    private Entity getParticipantEntity( String participantId, UUID studyId ) {
        EdmApi edmApi;
        try {
            ApiClient apiClient = apiClientCache.get( ApiClient.class );
            edmApi = apiClient.getEdmApi();
        } catch ( ExecutionException e ) {
            logger.error( "Unable to load apis." );
            return null;
        }

        SetMultimap<UUID, Object> participantData = HashMultimap.create();

        participantData.put( participantIdPropertyTypeId, participantId );
        UUID participantEntitySetId = edmApi
                .getEntitySetId( ChronicleServerUtil.getParticipantEntitySetName( studyId ) );
        EntityKey participantEntityKey = new EntityKey( participantEntitySetId,
                ApiUtil.generateDefaultEntityId( ImmutableList.of( participantIdPropertyTypeId ), participantData ) );
        return new Entity( participantEntityKey, participantData );
    }

    private Association getRecordedByAssociation( EntityKey src, EntityKey dst, OffsetDateTime timestamp ) {
        SetMultimap<UUID, Object> data = HashMultimap.create();
        data.put( dateLoggedPropertyTypeId, timestamp );
        EntityKey key = new EntityKey( recordedByEntitySetId,
                ApiUtil.generateDefaultEntityId( ImmutableList.of( dateLoggedPropertyTypeId ), data ) );
        return new Association( key, src, dst, data );
    }

    private Association getUsedByAssociation( EntityKey src, EntityKey dst ) {
        SetMultimap<UUID, Object> data = HashMultimap.create();
        data.put( stringIdPropertyTypeId, UUID.randomUUID() );
        EntityKey key = new EntityKey( usedByEntitySetId,
                ApiUtil.generateDefaultEntityId( ImmutableList.of( stringIdPropertyTypeId ), data ) );
        return new Association( key, src, dst, data );
    }

    //  TODO: add in throws exception!
    @Override
    public Integer logData(
            UUID studyId,
            String participantId,
            String deviceId,
            List<SetMultimap<UUID, Object>> data ) {

        DataIntegrationApi dataIntegrationApi;
        try {
            ApiClient apiClient = apiClientCache.get( ApiClient.class );
            dataIntegrationApi = apiClient.getDataIntegrationApi();
        } catch ( ExecutionException e ) {
            logger.error( "Unable to load apis." );
            return 0;
        }

        Set<Entity> entities = Sets.newHashSet();
        Set<Association> associations = Sets.newHashSet();

        Entity deviceEntity = getDeviceEntity( deviceId, Optional.absent() );
        Entity participantEntity = getParticipantEntity( participantId, studyId );
        entities.add( deviceEntity );
        entities.add( participantEntity );

        Set<Entity> dataEntities = data.stream().map( dataDetails ->
                new Entity( new EntityKey( dataEntitySetId,
                        ApiUtil.generateDefaultEntityId( StreamUtil.stream( dataKey ), dataDetails ) ), dataDetails )
        ).collect( Collectors.toSet() );
        entities.addAll( dataEntities );

        OffsetDateTime timestamp = OffsetDateTime.now();
        dataEntities.stream().forEach( dataEntity -> {
            associations.add( getRecordedByAssociation( dataEntity.getKey(), deviceEntity.getKey(), timestamp ) );
            associations.add( getRecordedByAssociation( dataEntity.getKey(), participantEntity.getKey(), timestamp ) );
        } );

        dataIntegrationApi.integrateEntityAndAssociationData( new BulkDataCreation( entities, associations ), false );

        //  TODO:s Make sure to return any errors??? Currently void method.
        return data.size();
    }

    @Override
    public UUID registerDatasource(
            UUID studyId,
            String participantId,
            String datasourceId,
            Optional<Datasource> datasource ) {

        //  previous logic already verified the participant and that the device is not already connected.
        //  add the device and associate to the participant and to the study
        //  this will be two associations device --> person, device --> study
        //  aka write a line to these association tables, createAssociationData() does not exist in lattice-js yet.
        //  DataApi.createEntityAndAssociationData() see example above for template

        DataApi dataApi;
        DataIntegrationApi dataIntegrationApi;
        SearchApi searchApi;
        try {
            ApiClient apiClient = apiClientCache.get( ApiClient.class );
            dataApi = apiClient.getDataApi();
            dataIntegrationApi = apiClient.getDataIntegrationApi();
            searchApi = apiClient.getSearchApi();
        } catch ( ExecutionException e ) {
            logger.error( "Unable to load apis." );
            return null;
        }

        studyInformation.computeIfAbsent( studyId, key -> HashMultimap.create() )
                .put( participantId, datasourceId );

        Entity deviceEntity = getDeviceEntity( datasourceId, datasource );
        Entity participantEntity = getParticipantEntity( participantId, studyId );
        Entity studyEntity = getStudyEntity( studyId );

        Set<Entity> entities = ImmutableSet.of( deviceEntity, participantEntity, studyEntity );
        Set<Association> associations = ImmutableSet
                .of( getUsedByAssociation( deviceEntity.getKey(), participantEntity.getKey() ),
                        getUsedByAssociation( deviceEntity.getKey(), studyEntity.getKey() ) );

        dataIntegrationApi.integrateEntityAndAssociationData( new BulkDataCreation( entities, associations ), false );

        return getDatasourceEntityKeyId( datasourceId, searchApi, dataApi );
    }

    @Override
    public UUID getDatasourceEntityKeyId( String datasourceId ) {
        DataApi dataApi;
        SearchApi searchApi;
        try {
            ApiClient apiClient = apiClientCache.get( ApiClient.class );
            dataApi = apiClient.getDataApi();
            searchApi = apiClient.getSearchApi();
        } catch ( ExecutionException e ) {
            logger.error( "Unable to load apis." );
            return null;
        }
        return getDatasourceEntityKeyId( datasourceId, searchApi, dataApi );
    }

    private UUID getDatasourceEntityKeyId( String datasourceId, SearchApi searchApi, DataApi dataApi ) {
        // HACK -- we should get entityKeyIds back from dataApi eventually
        DataSearchResult result = searchApi.executeEntitySetDataQuery( deviceEntitySetId,
                new SearchTerm( stringIdPropertyTypeId.toString() + ":\"" + datasourceId + "\"", 0, 1 ) );
        if ( result.getHits().size() == 0 ) {
            return null; // TODO do we want to throw an error here?
        }
        return UUID
                .fromString( result.getHits().iterator().next().get( INTERNAL_ID_FQN ).iterator().next().toString() );
    }

    @Override public boolean isKnownDatasource( UUID studyId, String participantId, String datasourceId ) {
        SetMultimap<String, String> participantDevices = Preconditions
                .checkNotNull( studyInformation.get( studyId ), "Study must exist." );

        return participantDevices.get( participantId ).contains( datasourceId );
    }

    @Override public boolean isKnownParticipant( UUID studyId, String participantId ) {
        return studyParticipants.get( studyId ).contains( participantId );
    }

    @Override public Map<String, UUID> getPropertyTypeIds( Set<String> propertyTypeFqns ) {
        EdmApi edmApi;
        try {
            ApiClient apiClient = apiClientCache.get( ApiClient.class );
            edmApi = apiClient.getEdmApi();
        } catch ( ExecutionException e ) {
            logger.error( "Unable to load EdmApi" );
            return ImmutableMap.of();
        }

        return propertyTypeFqns.stream().map( fqn -> new FullQualifiedName( fqn ) ).map( fqn -> Pair
                .of( fqn.getFullQualifiedNameAsString(),
                        edmApi.getPropertyTypeId( fqn.getNamespace(), fqn.getName() ) ) )
                .collect( Collectors.toMap( pair -> pair.getLeft(), pair -> pair.getRight() ) );
    }

    @Override
    public EntitySetData downloadStudyData( UUID studyId, String token ) {
        ApiClient apiClient = new ApiClient( RetrofitFactory.Environment.PRODUCTION, () -> token );
        try {
            EdmApi edmApi = apiClient.getEdmApi();
            DataApi dataApi = apiClient.getDataApi();
            SearchApi searchApi = apiClient.getSearchApi();

            LinkedHashSet<String> headers = new LinkedHashSet();
            headers.add( PERSON_ID_FQN.getFullQualifiedNameAsString() );
            headers.addAll( edmApi.getEntityType( edmApi.getEntitySet( dataEntitySetId ).getEntityTypeId() )
                    .getProperties().stream()
                    .map( ptId -> edmApi.getPropertyType( ptId ).getType().getFullQualifiedNameAsString() )
                    .collect( Collectors.toList() ) );

            UUID participantsEntitySetId = edmApi
                    .getEntitySetId( ChronicleServerUtil.getParticipantEntitySetName( studyId ) );

            Long numParticipants = dataApi.getEntitySetSize( participantsEntitySetId );
            List<SetMultimap<Object, Object>> participants = searchApi
                    .executeEntitySetDataQuery( participantsEntitySetId,
                            new SearchTerm( "*", 0, numParticipants.intValue() ) ).getHits();
            Map<UUID, Object> participantMap = participants.stream()
                    .filter( map -> map.containsKey( "id" ) && map
                            .containsKey( PERSON_ID_FQN.getFullQualifiedNameAsString() ) ).collect( Collectors
                            .toMap( map -> UUID.fromString( map.get( "id" ).iterator().next().toString() ),
                                    map -> map.get( PERSON_ID_FQN.getFullQualifiedNameAsString() ).iterator()
                                            .next() ) );

            Map<UUID, List<NeighborEntityDetails>> allParticipantNeighbors = searchApi
                    .executeEntityNeighborSearchBulk( participantsEntitySetId, participantMap.keySet() );

            Stream<SetMultimap<FullQualifiedName, Object>> dataStream = allParticipantNeighbors.entrySet().stream()
                    .flatMap( entry -> entry.getValue().stream()
                            .filter( neighbor -> neighbor.getNeighborEntitySet().isPresent() && neighbor
                                    .getNeighborEntitySet()
                                    .get().getId().equals( dataEntitySetId ) )
                            .map( neighbor -> {
                                SetMultimap<FullQualifiedName, Object> data = neighbor.getNeighborDetails().get();
                                data.put( PERSON_ID_FQN, participantMap.get( entry.getKey() ) );
                                return data;
                            } ) );

            return new EntitySetData( headers, dataStream::iterator );

        } catch ( ExecutionException e ) {
            logger.error( "Unable to load study data.", e );
            return new EntitySetData( new LinkedHashSet<>(), ImmutableList.of() );
        }
    }

    @Scheduled( fixedRate = 60000 )
    public void refreshStudyInformation() {
        DataApi dataApi;
        SearchApi searchApi;
        try {
            ApiClient apiClient = apiClientCache.get( ApiClient.class );
            dataApi = apiClient.getDataApi();
            searchApi = apiClient.getSearchApi();
        } catch ( ExecutionException e ) {
            logger.error( "Unable to load apis." );
            return;
        }

        Map<UUID, SetMultimap<String, String>> studyInformation = Maps.newConcurrentMap();
        SetMultimap<UUID, String> studyParticipants = HashMultimap.create();

        Long numStudies = dataApi.getEntitySetSize( studyEntitySetId );
        List<SetMultimap<FullQualifiedName, Object>> studySearchResult = searchApi
                .executeEntitySetDataQuery( studyEntitySetId, new SearchTerm( "*", 0, numStudies.intValue() ) )
                .getHits();

        Set<UUID> studyEntityKeyIds = studySearchResult.stream()
                .map( study -> UUID.fromString( study.get( INTERNAL_ID_FQN ).iterator().next().toString() ) )
                .collect( Collectors.toSet() );

        Map<UUID, List<NeighborEntityDetails>> studyNeighbors = searchApi
                .executeEntityNeighborSearchBulk( studyEntitySetId, studyEntityKeyIds );

        SetMultimap<UUID, UUID> participantEntityKeysByEntitySetId = Multimaps
                .synchronizedSetMultimap( HashMultimap.create() );

        studyNeighbors.values().stream().flatMap( list -> list.stream() )
                .parallel()
                .filter( neighbor -> neighbor.getNeighborEntitySet().isPresent() && neighbor.getNeighborEntitySet()
                        .get().getName().startsWith( ChronicleServerUtil.PARTICIPANTS_PREFIX ) )
                .forEach( neighbor -> participantEntityKeysByEntitySetId
                        .put( neighbor.getNeighborEntitySet().get().getId(), neighbor.getNeighborId().get() ) );

        Map<UUID, List<NeighborEntityDetails>> participantNeighbors = Maps.newConcurrentMap();

        participantEntityKeysByEntitySetId.asMap().entrySet().stream().parallel().forEach( entry -> participantNeighbors
                .putAll( searchApi
                        .executeEntityNeighborSearchBulk( entry.getKey(), Sets.newHashSet( entry.getValue() ) ) ) );

        // populate study information

        studySearchResult.forEach( studyObj -> {
            SetMultimap<String, String> participantsToDevices = HashMultimap.create();

            UUID studyId = UUID.fromString( studyObj.get( STRING_ID_FQN ).iterator().next().toString() );
            UUID studyEntityKeyId = UUID.fromString( studyObj.get( INTERNAL_ID_FQN ).iterator().next().toString() );

            if ( studyNeighbors.containsKey( studyEntityKeyId ) ) {
                studyNeighbors.get( studyEntityKeyId ).stream()
                        .filter( neighbor -> neighbor.getNeighborEntitySet().isPresent() && neighbor
                                .getNeighborEntitySet()
                                .get().getName().startsWith( ChronicleServerUtil.PARTICIPANTS_PREFIX ) )
                        .forEach( participantNeighbor -> {
                            String participantId = participantNeighbor.getNeighborDetails().get().get( PERSON_ID_FQN )
                                    .iterator()
                                    .next().toString();
                            UUID participantEntityKeyId = participantNeighbor.getNeighborId().get();

                            studyParticipants.put( studyId, participantId );
                            if ( participantNeighbors.containsKey( participantEntityKeyId ) ) {
                                Set<String> devices = participantNeighbors.get( participantEntityKeyId ).stream()
                                        .filter( neighbor -> neighbor.getNeighborEntitySet().isPresent() && neighbor
                                                .getNeighborEntitySet().get().getName()
                                                .equals( DEVICES_ENTITY_SET_NAME ) )
                                        .flatMap( neighbor -> neighbor.getNeighborDetails().get().get( STRING_ID_FQN )
                                                .stream() )
                                        .map( deviceId -> deviceId.toString() ).collect( Collectors.toSet() );

                                participantsToDevices.putAll( participantId, devices );
                            }

                        } );
            }

            studyInformation.put( studyId, participantsToDevices );
        } );

        this.studyInformation.clear();
        this.studyInformation.putAll( studyInformation );

        this.studyParticipants.clear();
        this.studyParticipants.putAll( studyParticipants );

    }

}