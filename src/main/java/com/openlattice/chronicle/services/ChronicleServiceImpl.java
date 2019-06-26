/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 */

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
import com.openlattice.authorization.securable.AbstractSecurableObject;
import com.openlattice.chronicle.ChronicleServerUtil;
import com.openlattice.chronicle.configuration.ChronicleConfiguration;
import com.openlattice.chronicle.constants.ParticipationStatus;
import com.openlattice.chronicle.sources.AndroidDevice;
import com.openlattice.chronicle.sources.Datasource;
import com.openlattice.client.ApiClient;
import com.openlattice.data.DataApi;
import com.openlattice.data.DataIntegrationApi;
import com.openlattice.data.EntityKey;
import com.openlattice.data.integration.Association;
import com.openlattice.data.integration.BulkDataCreation;
import com.openlattice.data.integration.Entity;
import com.openlattice.data.requests.NeighborEntityDetails;
import com.openlattice.edm.EdmApi;
import com.openlattice.edm.EntitySet;
import com.openlattice.search.SearchApi;
import com.openlattice.search.requests.DataSearchResult;
import com.openlattice.search.requests.EntityNeighborsFilter;
import com.openlattice.search.requests.SearchTerm;
import com.openlattice.shuttle.MissionControl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import javax.annotation.Nonnull;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.openlattice.chronicle.ChronicleServerUtil.PARTICIPANTS_PREFIX;

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
    private final String            PARTICIPATED_IN_AESN        = "chronicle_participated_in";
    private final String            SEARCH_PREFIX               = "entity";
    
    private final Set<UUID>         dataKey;
    private final FullQualifiedName STRING_ID_FQN               = new FullQualifiedName( "general.stringid" );
    private final FullQualifiedName PERSON_ID_FQN               = new FullQualifiedName( "nc.SubjectIdentification" );
    private final FullQualifiedName DATE_LOGGED_FQN             = new FullQualifiedName( "ol.datelogged" );
    private final FullQualifiedName STATUS_FQN                  = new FullQualifiedName( "ol.status" );
    private final FullQualifiedName VERSION_FQN                 = new FullQualifiedName( "ol.version" );
    private final FullQualifiedName MODEL_FQN                   = new FullQualifiedName( "vehicle.model" );
    private final UUID              studyEntitySetId;
    private final UUID              deviceEntitySetId;
    private final UUID              dataEntitySetId;
    private final UUID              recordedByEntitySetId;
    private final UUID              usedByEntitySetId;
    private final UUID              participatedInEntitySetId;
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
        participatedInEntitySetId = edmApi.getEntitySetId( PARTICIPATED_IN_AESN );

        stringIdPropertyTypeId = edmApi.getPropertyTypeId( STRING_ID_FQN.getNamespace(), STRING_ID_FQN.getName() );
        participantIdPropertyTypeId = edmApi.getPropertyTypeId( PERSON_ID_FQN.getNamespace(), PERSON_ID_FQN.getName() );
        dateLoggedPropertyTypeId = edmApi.getPropertyTypeId( DATE_LOGGED_FQN.getNamespace(), DATE_LOGGED_FQN.getName() );
        versionPropertyTypeId = edmApi.getPropertyTypeId( VERSION_FQN.getNamespace(), VERSION_FQN.getName() );
        modelPropertyTypeId = edmApi.getPropertyTypeId( MODEL_FQN.getNamespace(), MODEL_FQN.getName() );

        dataKey = edmApi.getEntityType( edmApi.getEntitySet( dataEntitySetId ).getEntityTypeId() ).getKey();

        refreshStudyInformation();

    }

    private Entity getDeviceEntity( String deviceId, Optional<Datasource> datasource ) {

        Map<UUID, Set<Object>> deviceData = new HashMap<>();
        deviceData.put( stringIdPropertyTypeId, Sets.newHashSet( deviceId ) );
        if ( datasource.isPresent() && AndroidDevice.class.isAssignableFrom( datasource.get().getClass() ) ) {
            AndroidDevice device = (AndroidDevice) datasource.get();
            deviceData.put( modelPropertyTypeId, Sets.newHashSet( device.getModel() ) );
            deviceData.put( versionPropertyTypeId, Sets.newHashSet( device.getOsVersion() ) );
        }
        EntityKey deviceEntityKey = new EntityKey(
                deviceEntitySetId,
                ApiUtil.generateDefaultEntityId( ImmutableList.of( stringIdPropertyTypeId ), deviceData )
        );
        return new Entity( deviceEntityKey, deviceData );
    }

    private Entity getStudyEntity( UUID studyId ) {

        Map<UUID, Set<Object>> studyData = new HashMap<>();
        studyData.put( stringIdPropertyTypeId, Sets.newHashSet( studyId.toString() ) );
        EntityKey studyEntityKey = new EntityKey(
                studyEntitySetId,
                ApiUtil.generateDefaultEntityId( ImmutableList.of( stringIdPropertyTypeId ), studyData )
        );
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

        Map<UUID, Set<Object>> participantData = new HashMap<>();
        participantData.put( participantIdPropertyTypeId, Sets.newHashSet( participantId ) );
        UUID participantEntitySetId = edmApi.getEntitySetId(
                ChronicleServerUtil.getParticipantEntitySetName( studyId )
        );
        EntityKey participantEntityKey = new EntityKey(
                participantEntitySetId,
                ApiUtil.generateDefaultEntityId( ImmutableList.of( participantIdPropertyTypeId ), participantData )
        );
        return new Entity( participantEntityKey, participantData );
    }

    private Association getRecordedByAssociation( EntityKey src, EntityKey dst, OffsetDateTime timestamp ) {

        Map<UUID, Set<Object>> data = new HashMap<>();
        data.put( dateLoggedPropertyTypeId, Sets.newHashSet( timestamp ) );
        EntityKey key = new EntityKey(
                recordedByEntitySetId,
                ApiUtil.generateDefaultEntityId( ImmutableList.of( dateLoggedPropertyTypeId ), data )
        );
        return new Association( key, src, dst, data );
    }

    private Association getUsedByAssociation( EntityKey src, EntityKey dst ) {

        Map<UUID, Set<Object>> data = new HashMap<>();
        data.put( stringIdPropertyTypeId, Sets.newHashSet( UUID.randomUUID() ) );
        EntityKey key = new EntityKey(
                usedByEntitySetId,
                ApiUtil.generateDefaultEntityId( ImmutableList.of( stringIdPropertyTypeId ), data )
        );
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

        ParticipationStatus status = getParticipationStatus( studyId, participantId );
        if ( ParticipationStatus.NOT_ENROLLED.equals( status ) ) {
            logger.warn( "participantId = {} is not enrolled, ignoring data upload", participantId );
            return 0;
        }

        entities.add( deviceEntity );
        entities.add( participantEntity );

        Set<Entity> dataEntities = data
                .stream()
                .map( dataDetails ->
                        new Entity(
                                new EntityKey(
                                        dataEntitySetId,
                                        ApiUtil.generateDefaultEntityId(
                                                StreamUtil.stream( dataKey ),
                                                Multimaps.asMap( dataDetails )
                                        )
                                ),
                                dataDetails
                        )
                )
                .collect( Collectors.toSet() );
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
                new SearchTerm( SEARCH_PREFIX + "." + stringIdPropertyTypeId.toString() + ":\"" + datasourceId + "\"", 0, 1 ) );
        if ( result.getHits().size() == 0 ) {
            return null; // TODO do we want to throw an error here?
        }
        return UUID
                .fromString( result.getHits().iterator().next().get( INTERNAL_ID_FQN ).iterator().next().toString() );
    }

    @Override
    public boolean isKnownDatasource( UUID studyId, String participantId, String datasourceId ) {

        logger.info( "Checking isKnownDatasource, stuydId = {}, participantId = {}", studyId, participantId );

        SetMultimap<String, String> participantDevices = Preconditions
                .checkNotNull( studyInformation.get( studyId ), "Study must exist." );

        return participantDevices.get( participantId ).contains( datasourceId );
    }

    @Override
    public boolean isKnownParticipant( UUID studyId, String participantId ) {
        return studyParticipants.get( studyId ).contains( participantId );
    }

    @Override
    public Map<String, UUID> getPropertyTypeIds( Set<String> propertyTypeFqns ) {
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

    @Scheduled( fixedRate = 60000 )
    public void refreshStudyInformation() {
        EdmApi edmApi;
        SearchApi searchApi;
        try {
            ApiClient apiClient = apiClientCache.get( ApiClient.class );
            edmApi = apiClient.getEdmApi();
            searchApi = apiClient.getSearchApi();
        } catch ( ExecutionException e ) {
            logger.error( "Unable to load apis." );
            return;
        }

        logger.info( "Refreshing study info..." );

        Map<UUID, SetMultimap<String, String>> studyInformation = Maps.newConcurrentMap();
        SetMultimap<UUID, String> studyParticipants = HashMultimap.create();

        List<SetMultimap<FullQualifiedName, Object>> studySearchResult = searchApi
                .executeEntitySetDataQuery( studyEntitySetId, new SearchTerm( "*", 0, SearchApi.MAX_SEARCH_RESULTS ) )
                .getHits();

        Set<UUID> studyEntityKeyIds = studySearchResult.stream()
                .map( study -> UUID.fromString( study.get( INTERNAL_ID_FQN ).iterator().next().toString() ) )
                .collect( Collectors.toSet() );

        Set<UUID> participantEntitySetIds = StreamUtil.stream( edmApi.getEntitySets() )
                .filter( entitySet -> entitySet.getName().startsWith( PARTICIPANTS_PREFIX ) )
                .map( EntitySet::getId )
                .collect( Collectors.toSet() );

        Map<UUID, List<NeighborEntityDetails>> studyNeighbors = searchApi.executeFilteredEntityNeighborSearch(
                studyEntitySetId,
                new EntityNeighborsFilter(
                        studyEntityKeyIds,
                        java.util.Optional.of( participantEntitySetIds ),
                        java.util.Optional.of( ImmutableSet.of() ),
                        java.util.Optional.of( ImmutableSet.of( participatedInEntitySetId ) )
                )
        );

        SetMultimap<UUID, UUID> participantEntityKeysByEntitySetId = Multimaps
                .synchronizedSetMultimap( HashMultimap.create() );

        studyNeighbors
                .values()
                .stream()
                .flatMap( Collection::stream )
                .parallel()
                .filter( neighbor -> neighbor.getNeighborEntitySet().isPresent() && neighbor.getNeighborId().isPresent() )
                .filter( neighbor -> neighbor.getNeighborEntitySet().get().getName().startsWith( PARTICIPANTS_PREFIX ) )
                .forEach( neighbor -> {
                    UUID participantEntitySetId = neighbor.getNeighborEntitySet().get().getId();
                    UUID participantEntityKeyId = neighbor.getNeighborId().get();
                    participantEntityKeysByEntitySetId.put( participantEntitySetId, participantEntityKeyId );
                } );

        Map<UUID, List<NeighborEntityDetails>> participantNeighbors = Maps.newConcurrentMap();

        participantEntityKeysByEntitySetId.asMap().entrySet().stream().parallel().forEach( entry -> participantNeighbors
                .putAll( searchApi
                        .executeFilteredEntityNeighborSearch( entry.getKey(),
                                new EntityNeighborsFilter( Sets.newHashSet( entry.getValue() ),
                                        java.util.Optional.of( ImmutableSet.of( deviceEntitySetId ) ),
                                                java.util.Optional.of( ImmutableSet.of() ),
                                                java.util.Optional.empty() ) ) ) );

        // populate study information

        studySearchResult.forEach( studyObj -> {
            SetMultimap<String, String> participantsToDevices = HashMultimap.create();

            UUID studyId = UUID.fromString( studyObj.get( STRING_ID_FQN ).iterator().next().toString() );
            UUID studyEntityKeyId = UUID.fromString( studyObj.get( INTERNAL_ID_FQN ).iterator().next().toString() );

            if ( studyNeighbors.containsKey( studyEntityKeyId ) ) {
                studyNeighbors.get( studyEntityKeyId ).stream()
                        .filter( neighbor -> neighbor.getNeighborEntitySet().isPresent() && neighbor
                                .getNeighborEntitySet()
                                .get()
                                .getName()
                                .startsWith( PARTICIPANTS_PREFIX )
                        )
                        .forEach( participantNeighbor -> {

                            Set<Object> participantIds = participantNeighbor.getNeighborDetails().get().get(PERSON_ID_FQN);
                            if ( participantIds.size() > 0 ) {

                                String participantId = participantIds.iterator().next().toString();
                                UUID participantEntityKeyId = participantNeighbor.getNeighborId().get();

                                studyParticipants.put( studyId, participantId );
                                if ( participantNeighbors.containsKey( participantEntityKeyId ) ) {
                                    Set<String> devices = participantNeighbors
                                            .get( participantEntityKeyId )
                                            .stream()
                                            .filter( neighbor -> neighbor.getNeighborEntitySet().isPresent() && neighbor
                                                    .getNeighborEntitySet()
                                                    .get()
                                                    .getName()
                                                    .equals( DEVICES_ENTITY_SET_NAME )
                                            )
                                            .flatMap( neighbor -> neighbor
                                                    .getNeighborDetails()
                                                    .get()
                                                    .get( STRING_ID_FQN )
                                                    .stream()
                                            )
                                            .map( deviceId -> deviceId.toString() ).collect( Collectors.toSet() );

                                    participantsToDevices.putAll( participantId, devices );
                                }
                            }
                        } );
            }

            studyInformation.put( studyId, participantsToDevices );
        } );

        this.studyInformation.clear();
        this.studyInformation.putAll( studyInformation );
        logger.info( "Updated studyInformation. Size = {}", this.studyInformation.size() );

        this.studyParticipants.clear();
        this.studyParticipants.putAll( studyParticipants );
        logger.info( "Updated studyParticipants. Size = {}", this.studyParticipants.size() );
    }

    @Override
    public Iterable<SetMultimap<String, Object>> getAllParticipantData( UUID studyId, UUID participantEntityKeyId ) {

        try {
            ApiClient apiClient = apiClientCache.get( ApiClient.class );
            EdmApi edmApi = apiClient.getEdmApi();
            SearchApi searchApi = apiClient.getSearchApi();

            String entitySetName = ChronicleServerUtil.getParticipantEntitySetName( studyId );
            UUID entitySetId = edmApi.getEntitySetId( entitySetName );
            if ( entitySetId == null ) {
                logger.error( "Unable to load participant EntitySet id." );
                return null;
            }

            List<NeighborEntityDetails> participantNeighbors = searchApi
                    .executeEntityNeighborSearch( entitySetId, participantEntityKeyId );

            return participantNeighbors
                    .stream()
                    .filter( neighbor -> neighbor.getNeighborDetails().isPresent()
                            && neighbor.getNeighborEntitySet().isPresent()
                            && DATA_ENTITY_SET_NAME.equals( neighbor.getNeighborEntitySet().get().getName() )
                    )
                    .map( neighbor -> {
                        neighbor.getNeighborDetails().get().removeAll( INTERNAL_ID_FQN );
                        SetMultimap<String, Object> neighborDetails = HashMultimap.create();
                        neighbor.getNeighborDetails().get()
                                .entries()
                                .forEach( e -> neighborDetails.put( e.getKey().toString(), e.getValue() ) );
                        return neighborDetails;
                    } )
                    .collect( Collectors.toSet() );
        } catch ( ExecutionException e ) {
            logger.error( "Unable to load participant data.", e );
            return null;
        }
    }

    @Override
    @Nonnull
    public SetMultimap<FullQualifiedName, Object> getParticipantEntity( UUID studyId, UUID participantEntityKeyId ) {

        try {
            ApiClient apiClient = apiClientCache.get( ApiClient.class );
            DataApi dataApi = apiClient.getDataApi();
            EdmApi edmApi = apiClient.getEdmApi();

            String entitySetName = ChronicleServerUtil.getParticipantEntitySetName( studyId );
            UUID entitySetId = edmApi.getEntitySetId( entitySetName );
            if ( entitySetId == null ) {
                logger.error( "Unable to load participant EntitySet id." );
                return ImmutableSetMultimap.of();
            }

            SetMultimap<FullQualifiedName, Object> entity = dataApi.getEntity( entitySetId, participantEntityKeyId );
            if ( entity == null ) {
                logger.error( "Unable to get participant entity." );
                return ImmutableSetMultimap.of();
            }
            return entity;
        } catch ( ExecutionException e ) {
            logger.error( "Unable to get participant entity.", e );
            return ImmutableSetMultimap.of();
        }
    }

    @Override
    public UUID getParticipantEntityKeyId( String participantId, UUID participantsEntitySetId, SearchApi searchApi ) {
        DataSearchResult result = searchApi.executeEntitySetDataQuery(
                participantsEntitySetId,
                new SearchTerm( SEARCH_PREFIX + "." + participantIdPropertyTypeId.toString() + ":\"" + participantId + "\"", 0, 1 )
        );
        if ( result.getHits().size() != 1 ) {
            return null;
        }
        SetMultimap<FullQualifiedName, Object> data = result.getHits().iterator().next();
        if ( data.containsKey( INTERNAL_ID_FQN ) ) {
            return UUID.fromString( data.get( INTERNAL_ID_FQN ).iterator().next().toString() );
        }
        return null;
    }

    @Override
    public ParticipationStatus getParticipationStatus( UUID studyId, String participantId ) {

        EdmApi edmApi;
        SearchApi searchApi;

        try {
            ApiClient apiClient = apiClientCache.get( ApiClient.class );
            edmApi = apiClient.getEdmApi();
            searchApi = apiClient.getSearchApi();
        } catch ( ExecutionException e ) {
            logger.error( "Unable to load apis." );
            return ParticipationStatus.UNKNOWN;
        }

        String participantsEntitySetName = ChronicleServerUtil.getParticipantEntitySetName( studyId );
        UUID participantsEntitySetId = edmApi.getEntitySetId( participantsEntitySetName );
        if ( participantsEntitySetId == null ) {
            logger.error( "unable to get the participants EntitySet id for studyId = {}", studyId );
            return ParticipationStatus.UNKNOWN;
        }

        UUID participantEntityKeyId = getParticipantEntityKeyId( participantId, participantsEntitySetId, searchApi );
        if ( participantEntityKeyId == null ) {
            logger.error( "unable to load participant entity key id for participantId = {}", participantId );
            return ParticipationStatus.UNKNOWN;
        }

        Map<UUID, List<NeighborEntityDetails>> neighborResults = searchApi.executeFilteredEntityNeighborSearch(
                participantsEntitySetId,
                new EntityNeighborsFilter(
                        ImmutableSet.of( participantEntityKeyId ),
                        java.util.Optional.of( ImmutableSet.of() ),
                        java.util.Optional.of( ImmutableSet.of( studyEntitySetId ) ),
                        java.util.Optional.of( ImmutableSet.of( participatedInEntitySetId ) )
                )
        );

        Set<SetMultimap<FullQualifiedName, Object>> target = neighborResults
                .get( participantEntityKeyId )
                .stream()
                .filter( neighborResult -> {
                    if ( neighborResult.getNeighborDetails().isPresent() ) {
                        try {
                            Set<Object> data = neighborResult.getNeighborDetails().get().get( STRING_ID_FQN );
                            UUID expectedStudyId = UUID.fromString( data.iterator().next().toString() );
                            return studyId.equals( expectedStudyId );
                        } catch ( Exception e ) {
                            logger.error( "caught exception while filtering by study id", e );
                        }
                    }
                    return false;
                } )
                .map( NeighborEntityDetails::getAssociationDetails )
                .collect( Collectors.toSet() );

        if ( target.size() == 1 ) {
            try {
                SetMultimap<FullQualifiedName, Object> data = target.iterator().next();
                if ( data.containsKey( STATUS_FQN ) ) {
                    Set<Object> statusValue = data.get( STATUS_FQN );
                    if ( statusValue != null && statusValue.size() == 1 ) {
                        String status = statusValue.iterator().next().toString();
                        return ParticipationStatus.valueOf( status );
                    }
                }
            } catch ( Exception e ) {
                logger.error( "unable to determine participation status", e );
            }
        } else {
            logger.error( "only one edge is expected between the participant and the study, found {}", target.size() );
        }

        return ParticipationStatus.UNKNOWN;
    }
}
