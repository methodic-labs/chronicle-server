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

import com.auth0.exception.Auth0Exception;
import com.dataloom.streams.StreamUtil;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.*;
import com.google.common.eventbus.EventBus;
import com.openlattice.ApiUtil;
import com.openlattice.chronicle.configuration.ChronicleConfiguration;
import com.openlattice.chronicle.constants.RecordType;
import com.openlattice.chronicle.data.ChronicleAppsUsageDetails;
import com.openlattice.chronicle.data.ChronicleQuestionnaire;
import com.openlattice.chronicle.data.DeleteType;
import com.openlattice.chronicle.data.ParticipationStatus;
import com.openlattice.chronicle.sources.AndroidDevice;
import com.openlattice.chronicle.sources.Datasource;
import com.openlattice.client.ApiClient;
import com.openlattice.data.*;
import com.openlattice.data.requests.FileType;
import com.openlattice.data.requests.NeighborEntityDetails;
import com.openlattice.data.requests.NeighborEntityIds;
import com.openlattice.directory.PrincipalApi;
import com.openlattice.edm.EdmApi;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.set.EntitySetPropertyMetadata;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.entitysets.EntitySetsApi;
import com.openlattice.search.SearchApi;
import com.openlattice.search.requests.EntityNeighborsFilter;
import com.openlattice.search.requests.SearchTerm;
import com.openlattice.shuttle.MissionControl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import javax.annotation.Nonnull;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.openlattice.chronicle.ChronicleServerUtil.getParticipantEntitySetName;
import static com.openlattice.chronicle.constants.EdmConstants.*;
import static com.openlattice.chronicle.constants.OutputConstants.*;
import static com.openlattice.edm.EdmConstants.ID_FQN;

public class ChronicleServiceImpl implements ChronicleService {
    protected static final Logger logger = LoggerFactory.getLogger( ChronicleServiceImpl.class );

    // studyId -> participantId -> deviceID -> device EKID
    private final Map<UUID, Map<String, Map<String, UUID>>> studyDevices = new HashMap<>();

    // studyId -> participantId -> participant EKID
    private final Map<UUID, Map<String, UUID>> studyParticipants = new HashMap<>();

    // studyId -> study EKID
    private final Map<UUID, UUID> studies = new HashMap<>();

    private final Map<String, String> userAppsDict                  = Collections.synchronizedMap( new HashMap<>() );
    private final Set<UUID>           notificationEnabledStudyEKIDs = new HashSet<>();

    private final ImmutableMap<UUID, PropertyType>      propertyTypesById;
    private final ImmutableMap<String, UUID>            entitySetIdMap;
    private final ImmutableMap<FullQualifiedName, UUID> propertyTypeIdsByFQN;

    private final String username;
    private final String password;

    private transient LoadingCache<Class<?>, ApiClient> apiClientCache = null;

    public ChronicleServiceImpl(
            EventBus eventBus,
            ChronicleConfiguration chronicleConfiguration ) throws ExecutionException {
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
        EntitySetsApi entitySetsApi = apiClient.getEntitySetsApi();

        // get entity setId map
        entitySetIdMap = ImmutableMap.copyOf( entitySetsApi.getEntitySetIds( ENTITY_SET_NAMES ) );

        // get propertyTypeId map
        Iterable<PropertyType> propertyTypes = edmApi.getPropertyTypes();
        propertyTypesById = StreamSupport
                .stream( propertyTypes.spliterator(), false )
                .collect( ImmutableMap.toImmutableMap( PropertyType::getId, Function.identity() ) );
        propertyTypeIdsByFQN = propertyTypesById
                .values()
                .stream()
                .collect( ImmutableMap.toImmutableMap( PropertyType::getType, PropertyType::getId ) );

        refreshStudyInformation();
        refreshUserAppsDictionary();
    }

    private UUID reserveEntityKeyId(
            UUID entitySetId,
            List<UUID> keyPropertyTypeIds,
            Map<UUID, Set<Object>> data,
            DataIntegrationApi dataIntegrationApi ) {

        ImmutableSet<EntityKey> entityKeys = ImmutableSet.of( new EntityKey(
                entitySetId,
                ApiUtil.generateDefaultEntityId( keyPropertyTypeIds, data )
        ) );

        return dataIntegrationApi.getEntityKeyIds( entityKeys ).iterator().next();
    }

    private UUID reserveDeviceEntityKeyId(
            Map<UUID, Set<Object>> data,
            DataIntegrationApi dataIntegrationApi ) {

        return reserveEntityKeyId(
                entitySetIdMap.get( DEVICES_ENTITY_SET_NAME ),
                ImmutableList.of( propertyTypeIdsByFQN.get( STRING_ID_FQN ) ),
                data,
                dataIntegrationApi
        );
    }

    private UUID getStudyEntityKeyId( UUID studyId ) {
        logger.info( "Retrieving studyEntityKeyId, studyId = {}", studyId );
        if ( studies.containsKey( studyId ) ) {
            return studies.get( studyId );
        }

        logger.error( "Failed to retrieve studyEntityKeyId, studyId = {}", studyId );
        return null;
    }

    private UUID getParticipantEntitySetId( UUID studyId ) {
        EntitySetsApi entitySetsApi;
        try {
            ApiClient apiClient = apiClientCache.get( ApiClient.class );
            entitySetsApi = apiClient.getEntitySetsApi();
        } catch ( ExecutionException e ) {
            logger.error( "Unable to load apis." );
            return null;
        }

        return entitySetsApi.getEntitySetId(
                getParticipantEntitySetName( studyId )
        );

    }

    private UUID getParticipantEntityKeyId( String participantId, UUID studyId ) {
        if ( studyParticipants.containsKey( studyId ) ) {
            Map<String, UUID> participantIdToEKMap = studyParticipants.get( studyId );

            if ( participantIdToEKMap.containsKey( participantId ) ) {
                return participantIdToEKMap.get( participantId );
            } else {
                logger.error( "Unable to get participantEntityKeyId. participant {} not associated with studyId {} ",
                        participantId,
                        studyId );
            }
        } else {
            logger.error( "Unable to get participantEntityKeyId of participantId {}. StudyId {}  not found ",
                    participantId,
                    studyId );
        }

        return null;
    }

    // return an OffsetDateTime with time 00:00
    private String getMidnightDateTime( String dateTime ) {
        return OffsetDateTime
                .parse( dateTime )
                .withHour( 0 )
                .withMinute( 0 )
                .withSecond( 0 )
                .withNano( 0 )
                .toString();
    }

    // unique for user + app + date
    private UUID reserveUsedByEntityKeyId(
            Map<UUID, Set<Object>> entityData,
            String appPackageName,
            String participantId,
            DataIntegrationApi dataIntegrationApi ) {

        Map<UUID, Set<Object>> data = new HashMap<>( entityData );
        data.put( propertyTypeIdsByFQN.get( FULL_NAME_FQN ), ImmutableSet.of( appPackageName ) );
        data.put( propertyTypeIdsByFQN.get( PERSON_ID_FQN ), Sets.newHashSet( participantId ) );

        return reserveEntityKeyId(
                entitySetIdMap.get( USED_BY_ENTITY_SET_NAME ),
                ImmutableList.of(
                        propertyTypeIdsByFQN.get( FULL_NAME_FQN ),
                        propertyTypeIdsByFQN.get( DATE_TIME_FQN ),
                        propertyTypeIdsByFQN.get( PERSON_ID_FQN )
                ),
                data,
                dataIntegrationApi
        );
    }

    // unique for app + device + date
    private UUID reserveRecordedByEntityKeyId(
            Map<UUID, Set<Object>> recordedByEntity,
            String appPackageName,
            DataIntegrationApi dataIntegrationApi ) {

        Map<UUID, Set<Object>> data = new HashMap<>( recordedByEntity );
        data.put( propertyTypeIdsByFQN.get( FULL_NAME_FQN ), Sets.newHashSet( appPackageName ) );

        return reserveEntityKeyId(
                entitySetIdMap.get( RECORDED_BY_ENTITY_SET_NAME ),
                ImmutableList.of(
                        propertyTypeIdsByFQN.get( DATE_LOGGED_FQN ),
                        propertyTypeIdsByFQN.get( STRING_ID_FQN ),
                        propertyTypeIdsByFQN.get( FULL_NAME_FQN )
                ),
                data,
                dataIntegrationApi
        );
    }

    private UUID reserveUserAppEntityKeyId(
            Map<UUID, Set<Object>> entityData,
            DataIntegrationApi dataIntegrationApi ) {

        return reserveEntityKeyId(
                entitySetIdMap.get( CHRONICLE_USER_APPS ),
                ImmutableList.of( propertyTypeIdsByFQN.get( FULL_NAME_FQN ) ),
                entityData,
                dataIntegrationApi
        );
    }

    private UUID reserveMetadataEntityKeyId(
            Map<UUID, Set<Object>> entityData,
            DataIntegrationApi dataIntegrationApi ) {

        return reserveEntityKeyId(
                entitySetIdMap.get( METADATA_ENTITY_SET_NAME ),
                ImmutableList.of( propertyTypeIdsByFQN.get( OL_ID_FQN ) ),
                entityData,
                dataIntegrationApi
        );
    }

    private UUID reserveHasEntityKeyId(
            Map<UUID, Set<Object>> entityData,
            DataIntegrationApi dataIntegrationApi ) {

        return reserveEntityKeyId(
                entitySetIdMap.get( HAS_ENTITY_SET_NAME ),
                ImmutableList.of( propertyTypeIdsByFQN.get( OL_ID_FQN ) ),
                entityData,
                dataIntegrationApi
        );
    }

    private void createUserAppsEntitiesAndAssociations(
            DataApi dataApi,
            DataIntegrationApi dataIntegrationApi,
            List<SetMultimap<UUID, Object>> data,
            UUID deviceEntityKeyId,
            UUID participantEntitySetId,
            UUID participantEntityKeyId,
            String participantId,
            String deviceId ) {

        /*
         * Most of the data pushed by devices does not correspond to apps that were visible in the UI.
         * Here we will only record the apps that exist in the chronicle user apps dictionary
         *
         */

        int numAppsUploaded = 0;
        for ( SetMultimap<UUID, Object> appEntity : data ) {
            try {
                Set<DataEdgeKey> dataEdgeKeys = new HashSet<>();

                String appPackageName = appEntity.get( propertyTypeIdsByFQN.get( FULL_NAME_FQN ) ).iterator().next()
                        .toString();
                String appName = userAppsDict.get( appPackageName );
                if ( appName == null )
                    continue;
                String dateLogged = getMidnightDateTime( appEntity.get( propertyTypeIdsByFQN.get( DATE_LOGGED_FQN ) )
                        .iterator().next()
                        .toString() );

                // create entity in chronicle_user_apps
                Map<UUID, Set<Object>> userAppEntityData = new HashMap<>();
                userAppEntityData.put( propertyTypeIdsByFQN.get( FULL_NAME_FQN ), Sets.newHashSet( appPackageName ) );
                userAppEntityData.put( propertyTypeIdsByFQN.get( TITLE_FQN ), Sets.newHashSet( appName ) );

                UUID userAppEntityKeyId = reserveUserAppEntityKeyId( userAppEntityData, dataIntegrationApi );
                dataApi.updateEntitiesInEntitySet( entitySetIdMap.get( CHRONICLE_USER_APPS ),
                        ImmutableMap.of( userAppEntityKeyId, userAppEntityData ),
                        UpdateType.Merge );

                // association: chronicle_user_apps => chronicle_recorded_by => chronicle_device
                Map<UUID, Set<Object>> recordedByEntityData = new HashMap<>();
                recordedByEntityData.put( propertyTypeIdsByFQN.get( DATE_LOGGED_FQN ), ImmutableSet.of( dateLogged ) );
                recordedByEntityData.put( propertyTypeIdsByFQN.get( STRING_ID_FQN ), ImmutableSet.of( deviceId ) );

                UUID recordedByEntityKeyId = reserveRecordedByEntityKeyId(
                        recordedByEntityData,
                        appPackageName,
                        dataIntegrationApi
                );
                dataApi.updateEntitiesInEntitySet(
                        entitySetIdMap.get( RECORDED_BY_ENTITY_SET_NAME ),
                        ImmutableMap.of( recordedByEntityKeyId, recordedByEntityData ),
                        UpdateType.Merge
                );

                EntityDataKey src = new EntityDataKey( entitySetIdMap.get( CHRONICLE_USER_APPS ), userAppEntityKeyId );
                EntityDataKey dst = new EntityDataKey(
                        entitySetIdMap.get( DEVICES_ENTITY_SET_NAME ),
                        deviceEntityKeyId
                );
                EntityDataKey edge = new EntityDataKey(
                        entitySetIdMap.get( RECORDED_BY_ENTITY_SET_NAME ),
                        recordedByEntityKeyId
                );

                dataEdgeKeys.add( new DataEdgeKey( src, dst, edge ) );

                // association: chronicle_user_apps => chronicle_used_by => chronicle_participants_{studyId}
                Map<UUID, Set<Object>> usedByEntityData = new HashMap<>();
                usedByEntityData.put( propertyTypeIdsByFQN.get( DATE_TIME_FQN ), ImmutableSet.of( dateLogged ) );

                UUID usedByEntityKeyId = reserveUsedByEntityKeyId( usedByEntityData,
                        appPackageName,
                        participantId,
                        dataIntegrationApi );
                dataApi.updateEntitiesInEntitySet(
                        entitySetIdMap.get( USED_BY_ENTITY_SET_NAME ),
                        ImmutableMap.of( usedByEntityKeyId, usedByEntityData ),
                        UpdateType.Merge
                );

                dst = new EntityDataKey( participantEntitySetId, participantEntityKeyId );
                edge = new EntityDataKey( entitySetIdMap.get( USED_BY_ENTITY_SET_NAME ), usedByEntityKeyId );
                dataEdgeKeys.add( new DataEdgeKey( src, dst, edge ) );
                dataApi.createEdges( dataEdgeKeys );

                numAppsUploaded++;
            } catch ( Exception exception ) {
                logger.error( "Error logging entry {}", appEntity, exception );
            }
        }

        logger.info( "Uploaded user apps entries: size = {}, participantId = {}", numAppsUploaded, participantId );
    }

    private void updateParticipantMetadata(
            DataApi dataApi,
            DataIntegrationApi dataIntegrationApi,
            List<SetMultimap<UUID, Object>> data,
            UUID participantEntitySetId,
            UUID participantEntityKeyId,
            String participantId ) {

        /*
         * Creates or adds to an existing metadata entity, with general statistics (at this moment mostly datetimes)
         * about the data collection.
         */

        Set<OffsetDateTime> pushedDateTimes = new HashSet<>();

        data.forEach(
                entity -> {
                    // most date properties in the entity are of length 1
                    for ( Object date : entity.get( propertyTypeIdsByFQN.get( DATE_LOGGED_FQN ) ) ) {
                        OffsetDateTime parsedDateTime = OffsetDateTime
                                .parse( date.toString() );

                        // filter out problematic entities with dates in the sixties
                        if ( parsedDateTime.isAfter( MINIMUM_DATE ) ) {
                            pushedDateTimes.add( parsedDateTime );
                        }
                    }
                }
        );

        String firstDateTime = pushedDateTimes
                .stream()
                .min( OffsetDateTime::compareTo )
                .orElse( null )
                .toString();

        String lastDateTime = pushedDateTimes
                .stream()
                .max( OffsetDateTime::compareTo )
                .orElse( null )
                .toString();

        Set<Object> uniqueDates = pushedDateTimes
                .stream()
                .map( dt -> dt
                        .truncatedTo( ChronoUnit.DAYS )
                        .format( DateTimeFormatter.ISO_DATE_TIME ) )
                .collect( Collectors.toSet() );

        Map<UUID, Set<Object>> metadataEntityData = new HashMap<>();
        metadataEntityData.put( propertyTypeIdsByFQN.get( OL_ID_FQN ), Set.of( participantEntityKeyId ) );
        UUID metadataEntityKeyId = reserveMetadataEntityKeyId( metadataEntityData, dataIntegrationApi );

        Map<FullQualifiedName, Set<Object>> entity = dataApi
                .getEntity( entitySetIdMap.get( METADATA_ENTITY_SET_NAME ), metadataEntityKeyId );
        metadataEntityData.put( propertyTypeIdsByFQN.get( START_DATE_TIME_FQN ),
                entity.getOrDefault( propertyTypeIdsByFQN.get( START_DATE_TIME_FQN ), Set.of( firstDateTime ) ) );
        metadataEntityData.put( propertyTypeIdsByFQN.get( END_DATE_TIME_FQN ), Set.of( lastDateTime ) );
        uniqueDates.addAll( entity.getOrDefault( RECORDED_DATE_TIME_FQN, Set.of() ) );
        metadataEntityData.put( propertyTypeIdsByFQN.get( RECORDED_DATE_TIME_FQN ), uniqueDates );

        dataApi.updateEntitiesInEntitySet( entitySetIdMap.get( METADATA_ENTITY_SET_NAME ),
                ImmutableMap.of( metadataEntityKeyId, metadataEntityData ),
                UpdateType.PartialReplace );

        Map<UUID, Set<Object>> hasEntityData = new HashMap<>();
        hasEntityData.put( propertyTypeIdsByFQN.get( OL_ID_FQN ), Set.of( firstDateTime ) );
        UUID hasEntityKeyId = reserveHasEntityKeyId( metadataEntityData, dataIntegrationApi );
        dataApi.updateEntitiesInEntitySet( entitySetIdMap.get( HAS_ENTITY_SET_NAME ),
                ImmutableMap.of( hasEntityKeyId, hasEntityData ),
                UpdateType.PartialReplace );

        EntityDataKey dst = new EntityDataKey( entitySetIdMap.get( METADATA_ENTITY_SET_NAME ), metadataEntityKeyId );
        EntityDataKey edge = new EntityDataKey( entitySetIdMap.get( HAS_ENTITY_SET_NAME ), hasEntityKeyId );
        EntityDataKey src = new EntityDataKey( participantEntitySetId, participantEntityKeyId );
        DataEdgeKey dataEdgeKey = new DataEdgeKey( src, dst, edge );
        dataApi.createEdges( Set.of( dataEdgeKey ) );

        logger.info( "Uploaded user metadata entries: participantId = {}", participantId );
    }

    private void createAppDataEntitiesAndAssociations(
            DataApi dataApi,
            List<SetMultimap<UUID, Object>> data,
            UUID deviceEntityKeyId,
            String participantId,
            UUID participantEntityKeyId,
            UUID participantEntitySetId ) {

        ListMultimap<UUID, Map<UUID, Set<Object>>> entities = ArrayListMultimap.create();
        ListMultimap<UUID, DataAssociation> associations = ArrayListMultimap.create();

        OffsetDateTime timeStamp = OffsetDateTime.now();

        for ( int i = 0; i < data.size(); i++ ) {
            entities.put( entitySetIdMap.get( DATA_ENTITY_SET_NAME ), Multimaps.asMap( data.get( i ) ) );

            Map<UUID, Set<Object>> recordedByEntity = ImmutableMap
                    .of( propertyTypeIdsByFQN.get( DATE_LOGGED_FQN ), Sets.newHashSet( timeStamp ) );

            associations.put( entitySetIdMap.get( RECORDED_BY_ENTITY_SET_NAME ), new DataAssociation(
                    entitySetIdMap.get( DATA_ENTITY_SET_NAME ),
                    java.util.Optional.of( i ),
                    java.util.Optional.empty(),
                    entitySetIdMap.get( DEVICES_ENTITY_SET_NAME ),
                    java.util.Optional.empty(),
                    java.util.Optional.of( deviceEntityKeyId ),
                    recordedByEntity
            ) );

            associations.put( entitySetIdMap.get( RECORDED_BY_ENTITY_SET_NAME ), new DataAssociation(
                    entitySetIdMap.get( DATA_ENTITY_SET_NAME ),
                    java.util.Optional.of( i ),
                    java.util.Optional.empty(),
                    participantEntitySetId,
                    java.util.Optional.empty(),
                    java.util.Optional.of( participantEntityKeyId ),
                    recordedByEntity
            ) );

        }

        DataGraph dataGraph = new DataGraph( entities, associations );
        dataApi.createEntityAndAssociationData( dataGraph );

        logger.info( "Uploaded data to chronicle_app_data: size: {},  participantId = {}",
                data.size(), participantId );
    }

    // update chronicle_user_apps -> chronicle_used_by -> chronicle_participants_{studyID} associations when apps usage survey is submitted
    @Override
    public Integer updateAppsUsageAssociationData(
            UUID studyId,
            String participantId,
            Map<UUID, Map<FullQualifiedName, Set<Object>>> associationDetails ) {

        logger.info( "Updating apps usage associations: participantId = {}, studyId = {}", participantId, studyId );

        DataApi dataApi;
        EdmApi edmApi;
        try {
            ApiClient apiClient = apiClientCache.get( ApiClient.class );
            dataApi = apiClient.getDataApi();
            edmApi = apiClient.getEdmApi();
        } catch ( ExecutionException e ) {
            logger.error( "unable to load apis" );
            throw new IllegalStateException( "unable to load apis" );
        }

        boolean knownParticipant = isKnownParticipant( studyId, participantId );
        if ( !knownParticipant ) {
            logger.error( "unable to update apps usage association data because unknown participant = {}",
                    participantId );
            throw new IllegalArgumentException( "participant not found" );
        }

        // create association data
        Map<UUID, Map<UUID, Set<Object>>> associationData = new HashMap<>();

        associationDetails
                .forEach( ( entityKeyId, entity ) -> {
                    associationData.put( entityKeyId, new HashMap<>() );
                    entity.forEach( ( propertyTypeFQN, data ) -> {
                        UUID propertyTypeId = edmApi.getPropertyTypeId( propertyTypeFQN.getNamespace(),
                                propertyTypeFQN.getName() );
                        associationData.get( entityKeyId ).put( propertyTypeId, data );
                    } );
                } );

        // update association entities
        try {
            dataApi.updateEntitiesInEntitySet( entitySetIdMap.get( USED_BY_ENTITY_SET_NAME ),
                    associationData,
                    UpdateType.Replace );
        } catch ( Exception exception ) {
            logger.error( "error updating chronicle_used_by associations" );
            throw new IllegalStateException( "error updating chronicle_used_by associations" );
        }

        logger.info( "updated {} apps usage associations", associationDetails.size() );
        return associationDetails.size();
    }

    // return a list of all the apps used by a participant filtered by the current date
    @Override
    public List<ChronicleAppsUsageDetails> getParticipantAppsUsageData(
            UUID studyId,
            String participantId,
            String date ) {

        logger.info( "Retrieving user apps: participantId = {}, studyId = {}", participantId, studyId );

        SearchApi searchApi;
        try {
            ApiClient apiClient = apiClientCache.get( ApiClient.class );
            searchApi = apiClient.getSearchApi();
        } catch ( ExecutionException e ) {
            logger.error( "Unable to load apis" );
            throw new IllegalStateException( e );
        }

        try {
            LocalDate.parse( date );
        } catch ( DateTimeParseException e ) {
            throw new IllegalArgumentException( "invalid date: " + date );
        }

        UUID participantEntityKeyId = getParticipantEntityKeyId( participantId, studyId );
        if ( participantEntityKeyId == null ) {
            logger.error(
                    "getUserApps: error retrieving participant. participant = {}, studyId = {}",
                    participantId,
                    studyId
            );
            throw new IllegalArgumentException( "invalid participantId" );
        }

        UUID participantEntitySetId = getParticipantEntitySetId( studyId );
        if ( participantEntitySetId == null ) {
            logger.error(
                    "getUserApps: error getting participant entity set id: participant = {}, studyId = {}",
                    participantId,
                    studyId
            );
            throw new IllegalStateException( "unable to get the participant entity set id for the given study id" );
        }

        // search participant neighbors
        Map<UUID, List<NeighborEntityDetails>> participantNeighbors = searchApi.executeFilteredEntityNeighborSearch(
                participantEntitySetId,
                new EntityNeighborsFilter(
                        ImmutableSet.of( participantEntityKeyId ),
                        java.util.Optional.of( ImmutableSet.of( entitySetIdMap.get( CHRONICLE_USER_APPS ) ) ),
                        java.util.Optional.of( ImmutableSet.of( participantEntitySetId ) ),
                        java.util.Optional.of( ImmutableSet.of( entitySetIdMap.get( USED_BY_ENTITY_SET_NAME ) ) )
                )
        );

        if ( participantNeighbors.containsKey( participantEntityKeyId ) ) {
            return participantNeighbors.get( participantEntityKeyId )
                    .stream()
                    .filter( neighbor -> neighbor.getNeighborDetails().isPresent() )
                    .filter( neighbor -> neighbor
                            .getAssociationDetails()
                            .get( DATE_TIME_FQN )
                            .iterator()
                            .next()
                            .toString()
                            .startsWith( date )
                    )
                    .map( neighbor -> new ChronicleAppsUsageDetails(
                            neighbor.getNeighborDetails().get(),
                            neighbor.getAssociationDetails()
                    ) )
                    .collect( Collectors.toList() );
        }

        logger.warn( "no user apps found" );
        return ImmutableList.of();
    }

    //  TODO: add in throws exception!
    @Override
    public Integer logData(
            UUID studyId,
            String participantId,
            String deviceId,
            List<SetMultimap<UUID, Object>> data ) {

        DataApi dataApi;
        DataIntegrationApi dataIntegrationApi;
        try {
            ApiClient apiClient = apiClientCache.get( ApiClient.class );
            dataApi = apiClient.getDataApi();
            dataIntegrationApi = apiClient.getDataIntegrationApi();
        } catch ( ExecutionException e ) {
            logger.error( "Unable to load apis." );
            return 0;
        }

        UUID participantEntityKeyId = getParticipantEntityKeyId( participantId, studyId );
        if ( participantEntityKeyId == null ) {
            logger.error( "Unable to retrieve participantEntityKeyId, studyId = {}, participantId = {}",
                    studyId,
                    participantId );
            return 0;
        }

        ParticipationStatus status = getParticipationStatus( studyId, participantId );
        if ( ParticipationStatus.NOT_ENROLLED.equals( status ) ) {
            logger.warn( "participantId = {} is not enrolled, ignoring data upload", participantId );
            return 0;
        }

        UUID participantEntitySetId = getParticipantEntitySetId( studyId );
        UUID deviceEntityKeyId = getDeviceEntityKeyId( studyId, participantId, deviceId );

        createUserAppsEntitiesAndAssociations( dataApi,
                dataIntegrationApi,
                data,
                deviceEntityKeyId,
                participantEntitySetId,
                participantEntityKeyId,
                participantId,
                deviceId );
        createAppDataEntitiesAndAssociations( dataApi,
                data,
                deviceEntityKeyId,
                participantId,
                participantEntityKeyId,
                participantEntitySetId );
        updateParticipantMetadata( dataApi,
                dataIntegrationApi,
                data,
                participantEntitySetId,
                participantEntityKeyId,
                participantId );
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
        try {
            ApiClient apiClient = apiClientCache.get( ApiClient.class );
            dataApi = apiClient.getDataApi();
            dataIntegrationApi = apiClient.getDataIntegrationApi();
        } catch ( ExecutionException e ) {
            logger.error( "Unable to load apis." );
            return null;
        }

        // device entity data
        Map<UUID, Set<Object>> deviceData = new HashMap<>();
        deviceData.put( propertyTypeIdsByFQN.get( STRING_ID_FQN ), Sets.newHashSet( datasourceId ) );

        if ( datasource.isPresent() && AndroidDevice.class.isAssignableFrom( datasource.get().getClass() ) ) {
            AndroidDevice device = (AndroidDevice) datasource.get();
            deviceData.put( propertyTypeIdsByFQN.get( MODEL_FQN ), Sets.newHashSet( device.getModel() ) );
            deviceData.put( propertyTypeIdsByFQN.get( VERSION_FQN ), Sets.newHashSet( device.getOsVersion() ) );
        }

        UUID deviceEntityKeyId = reserveDeviceEntityKeyId( deviceData, dataIntegrationApi );
        if ( deviceEntityKeyId == null ) {
            logger.error( "Unable to reserve deviceEntityKeyId, dataSourceId = {}, studyId = {}, participantId = {}",
                    datasourceId,
                    studyId,
                    participantId );
            return null;
        }
        dataApi.updateEntitiesInEntitySet( entitySetIdMap.get( DEVICES_ENTITY_SET_NAME ),
                ImmutableMap.of( deviceEntityKeyId, deviceData ),
                UpdateType.Merge );

        EntityDataKey deviceEDK = new EntityDataKey( entitySetIdMap.get( DEVICES_ENTITY_SET_NAME ), deviceEntityKeyId );
        studyDevices.computeIfAbsent( studyId, key -> new HashMap<>() )
                .put( participantId, Map.of( datasourceId, deviceEntityKeyId ) );

        UUID participantEntitySetId = getParticipantEntitySetId( studyId );
        UUID participantEntityKeyId = getParticipantEntityKeyId( participantId, studyId );
        if ( participantEntityKeyId == null ) {
            logger.error( "Unable to retrieve participantEntityKeyId, studyId = {}, participantId = {}",
                    studyId,
                    participantId );
            return null;
        }
        EntityDataKey participantEDK = new EntityDataKey( participantEntitySetId, participantEntityKeyId );

        UUID studyEntityKeyId = getStudyEntityKeyId( studyId );
        if ( studyEntityKeyId == null ) {
            logger.error( "Unable to retrieve studyEntityKeyId, studyId = {}", studyId );
            return null;
        }
        EntityDataKey studyEDK = new EntityDataKey( entitySetIdMap.get( STUDY_ENTITY_SET_NAME ), studyEntityKeyId );

        ListMultimap<UUID, DataEdge> associations = ArrayListMultimap.create();

        Map<UUID, Set<Object>> usedByEntity = ImmutableMap
                .of( propertyTypeIdsByFQN.get( STRING_ID_FQN ), Sets.newHashSet( UUID.randomUUID() ) );

        associations.put( entitySetIdMap.get( USED_BY_ENTITY_SET_NAME ),
                new DataEdge( deviceEDK, participantEDK, usedByEntity ) );
        associations.put( entitySetIdMap.get( USED_BY_ENTITY_SET_NAME ),
                new DataEdge( deviceEDK, studyEDK, usedByEntity ) );

        dataApi.createAssociations( associations );

        return deviceEntityKeyId;
    }

    @Override
    public boolean isNotificationsEnabled( UUID studyId ) {
        logger.info( "Checking notifications enabled on studyId = {}", studyId );

        UUID studyEntityKeyId = getStudyEntityKeyId( studyId );

        if ( studyEntityKeyId != null ) {
            boolean enabled = notificationEnabledStudyEKIDs.contains( studyEntityKeyId );
            if ( enabled ) {
                logger.info( "Notifications enabled on study: studyId = {}, entityKeyId={}",
                        studyId,
                        studyEntityKeyId );
            } else {
                logger.info( "Notifications not enabled on study: studyId={}, entityKeyId={}",
                        studyId,
                        studyEntityKeyId );
            }

            return enabled;
        }
        return false;
    }

    @Override
    public UUID getDeviceEntityKeyId( UUID studyId, String participantId, String datasourceId ) {
        logger.info( "Getting device entity key id, studyId = {}, participantId = {}, datasourceId = {}",
                studyId,
                participantId,
                datasourceId );

        if ( isKnownDatasource( studyId, participantId, datasourceId ) ) {
            return studyDevices.get( studyId ).get( participantId ).get( datasourceId );
        }
        return null;
    }

    @Override
    public boolean isKnownDatasource( UUID studyId, String participantId, String datasourceId ) {

        Map<String, Map<String, UUID>> participantDevices = Preconditions
                .checkNotNull( studyDevices.get( studyId ), "Study must exist" );

        return participantDevices.containsKey( participantId )
                && participantDevices.get( participantId ).containsKey( datasourceId );
    }

    private void deleteStudyData(
            UUID studyId,
            java.util.Optional<String> participantId,
            com.openlattice.data.DeleteType deleteType,
            String userToken ) {
        try {
            // load api for actions authenticated by the user
            ApiClient userApiClient = new ApiClient( () -> userToken );
            SearchApi userSearchApi = userApiClient.getSearchApi();
            EntitySetsApi userEntitySetsApi = userApiClient.getEntitySetsApi();
            DataApi userDataApi = userApiClient.getDataApi();

            // load api for actions authenticated by chronicle
            // because of the way we do things right now, only the chronicle
            // user can have permissions to delete from the entity set.
            // To be replaced fully with user authentication after refactoring.
            ApiClient chronicleApiClient = apiClientCache.get( ApiClient.class );
            DataApi chronicleDataApi = chronicleApiClient.getDataApi();

            String participantsEntitySetName = getParticipantEntitySetName( studyId );
            UUID participantsEntitySetId = userEntitySetsApi.getEntitySetId( participantsEntitySetName );
            if ( participantsEntitySetId == null ) {
                throw new Exception( "unable to get the participants EntitySet id for studyId " + studyId );
            }

            // get a set of all participants to remove:
            Set<UUID> participantsToRemove = new HashSet<>();
            if ( participantId.isPresent() ) {
                // if participantId: add to set
                UUID participantEntityKeyId = getParticipantEntityKeyId( participantId.get(), studyId );
                if ( participantEntityKeyId == null ) {
                    throw new Exception(
                            "unable to delete participant " + participantId + ": participant does not exist." );
                }
                participantsToRemove.add( participantEntityKeyId );
            } else {
                // if no participant Id: load all participants and add to set
                chronicleDataApi
                        .loadEntitySetData( participantsEntitySetId, FileType.json, userToken )
                        .forEach( entity -> entity.get( ID_FQN ).forEach( personId ->
                                        participantsToRemove.add( UUID.fromString( personId.toString() ) )
                                )
                        );
            }

            // Be super careful here that the mapping is one-to-one:
            // don't delete neighbors that might have other neighbors/participants
            Set<UUID> srcNeighborSetIds = ImmutableSet.of(
                    entitySetIdMap.get( DEVICES_ENTITY_SET_NAME ),
                    entitySetIdMap.get( DATA_ENTITY_SET_NAME ),
                    entitySetIdMap.get( PREPROCESSED_DATA_ENTITY_SET_NAME )
            );
            Set<UUID> dstNeighborSetIds = ImmutableSet.of( entitySetIdMap.get( ANSWERS_ENTITY_SET_NAME ) );
            Map<UUID, Set<UUID>> toDeleteEntitySetIdEntityKeyId = new HashMap<>();

            // create a key for all entity sets
            Sets.union( srcNeighborSetIds, dstNeighborSetIds ).forEach( entitySetId -> {
                toDeleteEntitySetIdEntityKeyId.put( entitySetId, new HashSet<>() );
            } );

            participantsToRemove.forEach(
                    participantEntityKeyId -> {
                        // Get neighbors
                        Map<UUID, Map<UUID, SetMultimap<UUID, NeighborEntityIds>>> participantNeighbors = userSearchApi
                                .executeFilteredEntityNeighborIdsSearch(
                                        participantsEntitySetId,
                                        new EntityNeighborsFilter(
                                                Set.of( participantEntityKeyId ),
                                                java.util.Optional.of( srcNeighborSetIds ),
                                                java.util.Optional.of( dstNeighborSetIds ),
                                                java.util.Optional.empty()
                                        )
                                );

                        if ( participantNeighbors.size() == 0 ) {
                            logger.debug( "Attempt to remove participant without data." );
                        }

                        // fill Map<entitySetId, Set<entityKeyId>>
                        participantNeighbors
                                .getOrDefault( participantEntityKeyId, Map.of() )
                                .forEach( ( edgeEntitySetId, edgeNeighbor ) -> {
                                    edgeNeighbor.forEach( ( neighborEntitySetId, neighborEntityIds ) -> {
                                        toDeleteEntitySetIdEntityKeyId.get( neighborEntitySetId )
                                                .add( neighborEntityIds.getNeighborEntityKeyId() );
                                    } );
                                } );
                    }

            );

            // delete all neighbors
            toDeleteEntitySetIdEntityKeyId
                    .forEach(
                            ( entitySetId, entityKeyId ) -> chronicleDataApi
                                    .deleteEntities( entitySetId, entityKeyId, deleteType )
                    );

            // delete participants
            Integer deleted = userDataApi.deleteEntities( participantsEntitySetId, participantsToRemove, deleteType );
            logger.info( "Deleted {} entities for participant {}.", deleted, participantId );

            // delete study if no participantId is specified
            if ( participantId.isEmpty() ) {
                // delete participant entity set
                userEntitySetsApi.deleteEntitySet( participantsEntitySetId );
                logger.info( "Deleted participant dataset for study {}.", studyId );
                UUID studyEntityKeyId = getStudyEntityKeyId( studyId );
                userDataApi.deleteEntities( entitySetIdMap.get( STUDY_ENTITY_SET_NAME ),
                        ImmutableSet.of( studyEntityKeyId ),
                        deleteType );
                logger.info( "Deleted study {} from global studies dataset.", studyId );
            }

        } catch ( Exception e ) {
            String errorMsg = "failed to delete participant data";
            logger.error( errorMsg, e );
            throw new RuntimeException( errorMsg );
        }
    }

    @Override
    public void deleteParticipantAndAllNeighbors(
            UUID studyId,
            String participantId,
            DeleteType deleteType,
            String userToken ) {
        com.openlattice.data.DeleteType deleteTypeTransformed = com.openlattice.data.DeleteType
                .valueOf( deleteType.toString() );
        deleteStudyData( studyId, java.util.Optional.of( participantId ), deleteTypeTransformed, userToken );
        logger.info( "Successfully removed a participant from {}", studyId );
    }

    @Override
    public void deleteStudyAndAllNeighbors( UUID studyId, DeleteType deleteType, String userToken ) {
        com.openlattice.data.DeleteType deleteTypeTransformed = com.openlattice.data.DeleteType
                .valueOf( deleteType.toString() );
        deleteStudyData( studyId, java.util.Optional.empty(), deleteTypeTransformed, userToken );
        logger.info( "Successfully removed study {}", studyId );
    }

    @Override
    public boolean isKnownParticipant( UUID studyId, String participantId ) {
        return studyParticipants.getOrDefault( studyId, new HashMap<>() ).containsKey( participantId );
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

    // retrieve study entity key ids with daily notifications enabled
    private Set<UUID> getNotificationEnabledStudies(
            List<Map<FullQualifiedName, Set<Object>>> studies,
            SearchApi searchApi ) {
        Set<UUID> result = new HashSet<>();

        //mapping from studyEKID -> studyId
        Map<UUID, UUID> studyEntityKeyIdMap = studies
                .stream()
                .collect( Collectors.toMap(
                        study -> UUID.fromString( study.get( ID_FQN ).iterator().next().toString() ),
                        study -> UUID.fromString( study.get( STRING_ID_FQN ).iterator().next().toString() )
                ) );

        Set<UUID> studyEntityKeyIds = studyEntityKeyIdMap.keySet();

        // Get notification entities that neighbor study
        Map<UUID, List<NeighborEntityDetails>> studyNeighbors = searchApi
                .executeFilteredEntityNeighborSearch(
                        entitySetIdMap.get( STUDY_ENTITY_SET_NAME ),
                        new EntityNeighborsFilter(
                                studyEntityKeyIds,
                                java.util.Optional
                                        .of( ImmutableSet.of( entitySetIdMap.get( NOTIFICATION_ENTITY_SET_NAME ) ) ),
                                java.util.Optional.of( ImmutableSet.of( entitySetIdMap.get( STUDY_ENTITY_SET_NAME ) ) ),
                                java.util.Optional
                                        .of( ImmutableSet.of( entitySetIdMap.get( PART_OF_ENTITY_SET_NAME ) ) )
                        )
                );

        /*
         * studies with notifications enabled have 'ol.id' property in the corresponding
         * associationDetails set to the value of the studyId
         * For each study, there is can only be at most 1 notification -> partof -> study association,
         * therefore it suffices to explore the first neighbor
         */

        for ( Map.Entry<UUID, List<NeighborEntityDetails>> entry : studyNeighbors.entrySet() ) {

            List<NeighborEntityDetails> entityDetailsList = entry.getValue();
            UUID entityKeyId = entry.getKey();

            if ( !entityDetailsList.isEmpty() ) {
                NeighborEntityDetails details = entityDetailsList.get( 0 );

                String associationVal = details.getAssociationDetails()
                        .getOrDefault( OL_ID_FQN, Set.of( "" ) )
                        .iterator()
                        .next().toString();

                String studyId = studyEntityKeyIdMap.getOrDefault( entityKeyId, UUID.randomUUID() ).toString();
                if ( studyId.equals( associationVal ) ) {
                    result.add( entityKeyId );
                }

            }
        }
        return result;
    }

    @Scheduled( fixedRate = 60000 )
    public void refreshUserAppsDictionary() {

        DataApi dataApi;
        String jwtToken;

        try {
            ApiClient apiClient = apiClientCache.get( ApiClient.class );
            dataApi = apiClient.getDataApi();
            jwtToken = MissionControl.getIdToken( username, password );
        } catch ( ExecutionException | Auth0Exception e ) {
            logger.error( "Caught an exception", e );
            return;
        }

        logger.info( "Refreshing chronicle user apps dictionary" );

        Iterable<SetMultimap<FullQualifiedName, Object>> entitySetData = dataApi.loadEntitySetData(
                entitySetIdMap.get( USER_APPS_DICTIONARY ),
                FileType.json,
                jwtToken
        );
        logger.info(
                "Fetched {} items from user apps dictionary entity set",
                Iterators.size( entitySetData.iterator() )
        );

        Map<String, String> appsDict = new HashMap<>();
        entitySetData.forEach( entity -> {
            try {
                String packageName = null;
                if ( entity.containsKey( FULL_NAME_FQN ) && !entity.get( FULL_NAME_FQN ).isEmpty() ) {
                    packageName = entity.get( FULL_NAME_FQN ).iterator().next().toString();
                }

                String appName = null;
                if ( entity.containsKey( TITLE_FQN ) && !entity.get( TITLE_FQN ).isEmpty() ) {
                    appName = entity.get( TITLE_FQN ).iterator().next().toString();
                }

                String recordType = null;
                if ( entity.containsKey( RECORD_TYPE_FQN ) && !entity.get( RECORD_TYPE_FQN ).isEmpty() ) {
                    recordType = entity.get( RECORD_TYPE_FQN ).iterator().next().toString();
                }

                if ( !RecordType.SYSTEM.name().equals( recordType ) && packageName != null && appName != null ) {
                    appsDict.put( packageName, appName );
                }
            } catch ( Exception e ) {
                logger.error( "caught exception while processing entities from user apps dictionary", e );
            }
        } );

        this.userAppsDict.clear();
        this.userAppsDict.putAll( appsDict );

        logger.info( "Loaded {} items into user apps dictionary", appsDict.size() );
    }

    @Scheduled( fixedRate = 60000 )
    public void refreshStudyInformation() {
        EntitySetsApi entitySetsApi;
        SearchApi searchApi;
        PrincipalApi principalApi;
        try {
            ApiClient apiClient = apiClientCache.get( ApiClient.class );
            entitySetsApi = apiClient.getEntitySetsApi();
            searchApi = apiClient.getSearchApi();
            principalApi = apiClient.getPrincipalApi();
        } catch ( ExecutionException e ) {
            logger.error( "Unable to load apis." );
            return;
        }

        try {
            logger.info( "attempting to sync user..." );
            principalApi.syncCallingUser();
        } catch ( Exception e ) {
            logger.error( "failed to sync user" );
        }

        logger.info( "Refreshing study info..." );

        Map<UUID, Map<String, UUID>> studyParticipants = new HashMap<>();
        Map<UUID, Map<String, Map<String, UUID>>> studyDevices = new HashMap<>();
        Map<UUID, UUID> studies = new HashMap<>();

        List<Map<FullQualifiedName, Set<Object>>> studySearchResult = searchApi
                .executeEntitySetDataQuery( entitySetIdMap.get( STUDY_ENTITY_SET_NAME ),
                        new SearchTerm( "*", 0, SearchApi.MAX_SEARCH_RESULTS ) )
                .getHits();

        if ( studySearchResult.isEmpty() ) {
            logger.info( "No studies found." );
            return;
        }

        Set<UUID> studyEntityKeyIds = studySearchResult.stream()
                .map( study -> UUID.fromString( study.get( ID_FQN ).iterator().next().toString() ) )
                .collect( Collectors.toSet() );

        Set<UUID> participantEntitySetIds = StreamUtil.stream( entitySetsApi.getEntitySets() )
                .filter( entitySet -> entitySet.getName().startsWith( PARTICIPANTS_PREFIX ) )
                .map( EntitySet::getId )
                .collect( Collectors.toSet() );

        Map<UUID, List<NeighborEntityDetails>> studyNeighbors = searchApi.executeFilteredEntityNeighborSearch(
                entitySetIdMap.get( STUDY_ENTITY_SET_NAME ),
                new EntityNeighborsFilter(
                        studyEntityKeyIds,
                        java.util.Optional.of( participantEntitySetIds ),
                        java.util.Optional.of( ImmutableSet.of() ),
                        java.util.Optional.of( ImmutableSet.of( entitySetIdMap.get( PARTICIPATED_IN_AESN ) ) )
                )
        );

        SetMultimap<UUID, UUID> participantEntityKeysByEntitySetId = Multimaps
                .synchronizedSetMultimap( HashMultimap.create() );

        studyNeighbors
                .values()
                .stream()
                .flatMap( Collection::stream )
                .parallel()
                .filter( neighbor -> neighbor.getNeighborEntitySet().isPresent() && neighbor.getNeighborId()
                        .isPresent() )
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
                                        java.util.Optional
                                                .of( ImmutableSet.of( entitySetIdMap.get( DEVICES_ENTITY_SET_NAME ) ) ),
                                        java.util.Optional.of( ImmutableSet.of() ),
                                        java.util.Optional.empty() ) ) ) );

        // get studies with notifications enabled
        Set<UUID> notificationEnabledStudyEKIDs = getNotificationEnabledStudies( studySearchResult, searchApi );

        // populate study information

        studySearchResult.forEach( studyObj -> {
            Map<String, Map<String, UUID>> participantsToDevices = new HashMap<>();
            UUID studyId, studyEntityKeyId;

            try {
                studyId = UUID.fromString( studyObj.get( STRING_ID_FQN ).iterator().next().toString() );
                studyEntityKeyId = UUID.fromString( studyObj.get( ID_FQN ).iterator().next().toString() );
            } catch ( IllegalArgumentException error ) {
                logger.error( "invalid studyId : {}", studyObj.get( STRING_ID_FQN ).iterator().next().toString() );
                return; // skip the current study object
            }

            if ( studies.containsKey( studyId ) ) {
                logger.error( "encountered duplicate studyId = {}", studyId );
                return;
            } else {
                studies.put( studyId, studyEntityKeyId );
                studyParticipants.put( studyId, new HashMap<>() );
            }

            if ( studyNeighbors.containsKey( studyEntityKeyId ) ) {
                studyNeighbors.get( studyEntityKeyId ).stream()
                        .filter( neighbor -> neighbor.getNeighborEntitySet().isPresent() && neighbor
                                .getNeighborEntitySet()
                                .get()
                                .getName()
                                .startsWith( PARTICIPANTS_PREFIX )
                        )
                        .forEach( participantNeighbor -> {

                            Set<Object> participantIds = participantNeighbor.getNeighborDetails().get()
                                    .get( PERSON_ID_FQN );
                            if ( participantIds.size() > 0 ) {

                                String participantId = participantIds.iterator().next().toString();
                                UUID participantEntityKeyId = participantNeighbor.getNeighborId().get();

                                if ( studyParticipants.containsKey( studyId ) && studyParticipants.get( studyId )
                                        .containsKey( participantId ) ) {
                                    logger.error( "Encountered duplicate participantId = {} in studyId = {}",
                                            participantId,
                                            studyId );
                                } else {
                                    studyParticipants.get( studyId ).put( participantId, participantEntityKeyId );
                                    if ( participantNeighbors.containsKey( participantEntityKeyId ) ) {
                                        Map<String, UUID> devices = new HashMap<>();
                                        participantNeighbors
                                                .get( participantEntityKeyId )
                                                .stream()
                                                .filter( neighbor -> neighbor.getNeighborEntitySet().isPresent()
                                                        && neighbor
                                                        .getNeighborEntitySet()
                                                        .get()
                                                        .getName()
                                                        .equals( DEVICES_ENTITY_SET_NAME )
                                                ).forEach( neighbor -> {
                                            if ( neighbor.getNeighborDetails().isPresent() ) {
                                                String deviceId = neighbor.getNeighborDetails().get()
                                                        .get( STRING_ID_FQN ).iterator().next().toString();
                                                UUID deviceEntityKeyId = UUID
                                                        .fromString( neighbor.getNeighborDetails().get().get( ID_FQN )
                                                                .iterator().next().toString() );
                                                devices.put( deviceId, deviceEntityKeyId );
                                            }

                                        } );

                                        participantsToDevices.put( participantId, devices );
                                    }
                                }
                            }
                        } );
            }

            studyDevices.put( studyId, participantsToDevices );
        } );

        this.studies.clear();
        this.studies.putAll( studies );
        logger.info( "Updated studyInformation. Size = {}", this.studies.size() );

        this.studyParticipants.clear();
        this.studyParticipants.putAll( studyParticipants );
        logger.info( "Updated studyParticipants. Size = {}",
                this.studyParticipants.values().stream().flatMap( map -> map.values().stream() ).count() );

        this.studyDevices.clear();
        this.studyDevices.putAll( studyDevices );

        this.notificationEnabledStudyEKIDs.clear();
        this.notificationEnabledStudyEKIDs.addAll( notificationEnabledStudyEKIDs );
        logger.info( "Updated studies with notifications enabled. Size = {}",
                this.notificationEnabledStudyEKIDs.size() );
    }

    private Iterable<Map<String, Set<Object>>> getParticipantDataHelper(
            UUID studyId,
            UUID participantEntityKeyId,
            String edgeEntitySetName,
            String sourceEntitySetName,
            String token ) {

        try {
            ApiClient apiClient = new ApiClient( () -> token );
            EntitySetsApi entitySetsApi = apiClient.getEntitySetsApi();
            SearchApi searchApi = apiClient.getSearchApi();
            EdmApi edmApi = apiClient.getEdmApi();

            /*
             * 1. get the relevant EntitySets
             */

            String participantEntitySetName = getParticipantEntitySetName( studyId );
            Map<String, EntitySet> entitySetsByName = entitySetsApi.getEntitySetsByName(
                    Set.of( participantEntitySetName, sourceEntitySetName, edgeEntitySetName )
            );

            EntitySet participantsES = entitySetsByName.get( participantEntitySetName );
            EntitySet sourceES = entitySetsByName.get( sourceEntitySetName );
            EntitySet edgeES = entitySetsByName.get( edgeEntitySetName );

            /*
             * 2. get all PropertyTypes and set up maps for easy lookups
             */

            Map<UUID, PropertyType> propertyTypesById = StreamSupport
                    .stream( edmApi.getPropertyTypes().spliterator(), false )
                    .collect( Collectors.toMap( PropertyType::getId, Function.identity() ) );

            Map<UUID, Map<UUID, EntitySetPropertyMetadata>> meta =
                    entitySetsApi.getPropertyMetadataForEntitySets( Set.of( sourceES.getId(), edgeES.getId() ) );

            Map<UUID, EntitySetPropertyMetadata> sourceMeta = meta.get( sourceES.getId() );
            Map<UUID, EntitySetPropertyMetadata> edgeMeta = meta.get( edgeES.getId() );

            /*
             * 3. get EntitySet primary keys, which are used later for filtering
             */

            Set<FullQualifiedName> sourceKeys = edmApi.getEntityType( sourceES.getEntityTypeId() )
                    .getKey()
                    .stream()
                    .map( propertyTypeId -> propertyTypesById.get( propertyTypeId ).getType() )
                    .collect( Collectors.toSet() );

            Set<FullQualifiedName> edgeKeys = edmApi.getEntityType( edgeES.getEntityTypeId() )
                    .getKey()
                    .stream()
                    .map( propertyTypeId -> propertyTypesById.get( propertyTypeId ).getType() )
                    .collect( Collectors.toSet() );

            /*
             * 4. perform filtered search to get participant neighbors
             */

            Map<UUID, List<NeighborEntityDetails>> participantNeighbors = searchApi.executeFilteredEntityNeighborSearch(
                    participantsES.getId(),
                    new EntityNeighborsFilter(
                            Set.of( participantEntityKeyId ),
                            java.util.Optional.of( ImmutableSet.of( sourceES.getId() ) ),
                            java.util.Optional.of( ImmutableSet.of( participantsES.getId() ) ),
                            java.util.Optional.of( ImmutableSet.of( edgeES.getId() ) )
                    )
            );

            /*
             * 5. filter and clean the data before sending it back
             */

            return participantNeighbors
                    .getOrDefault( participantEntityKeyId, List.of() )
                    .stream()
                    .filter( neighbor -> neighbor.getNeighborDetails().isPresent() )
                    .map( neighbor -> {

                        Map<FullQualifiedName, Set<Object>> entityData = neighbor.getNeighborDetails().get();
                        entityData.remove( ID_FQN );

                        ZoneId tz = ZoneId.of( entityData
                                .getOrDefault( TIMEZONE_FQN, ImmutableSet.of( DEFAULT_TIMEZONE ) )
                                .iterator()
                                .next()
                                .toString()
                        );

                        Map<String, Set<Object>> cleanEntityData = Maps.newHashMap();
                        entityData
                                .entrySet()
                                .stream()
                                .filter( entry -> !sourceKeys.contains( entry.getKey() ) )
                                .forEach( entry -> {
                                    Set<Object> values = entry.getValue();
                                    PropertyType propertyType = propertyTypesById.get(
                                            propertyTypeIdsByFQN.get( entry.getKey() )
                                    );
                                    String propertyTitle = sourceMeta.get( propertyType.getId() ).getTitle();
                                    if ( propertyType.getDatatype() == EdmPrimitiveTypeKind.DateTimeOffset ) {
                                        Set<Object> dateTimeValues = values
                                                .stream()
                                                .map( value -> {
                                                    try {
                                                        return OffsetDateTime
                                                                .parse( value.toString() )
                                                                .toInstant()
                                                                .atZone( tz )
                                                                .toOffsetDateTime()
                                                                .toString();
                                                    } catch ( Exception e ) {
                                                        return null;
                                                    }
                                                } )
                                                .filter( StringUtils::isNotBlank )
                                                .collect( Collectors.toSet() );
                                        cleanEntityData.put( APP_PREFIX + propertyTitle, dateTimeValues );
                                    } else {
                                        cleanEntityData.put( APP_PREFIX + propertyTitle, values );
                                    }
                                } );

                        neighbor.getAssociationDetails().remove( ID_FQN );
                        neighbor.getAssociationDetails()
                                .entrySet()
                                .stream()
                                .filter( entry -> !edgeKeys.contains( entry.getKey() ) )
                                .forEach( entry -> {
                                    UUID propertyTypeId = propertyTypeIdsByFQN.get( entry.getKey() );
                                    String propertyTitle = edgeMeta.get( propertyTypeId ).getTitle();
                                    cleanEntityData.put( USER_PREFIX + propertyTitle, entry.getValue() );
                                } );

                        return cleanEntityData;
                    } )
                    .collect( Collectors.toSet() );
        } catch ( Exception e ) {
            // since the response is meant to be a file download, returning "null" will respond with 200 and return
            // an empty file, which is not what we want. the request should not "succeed" when something goes wrong
            // internally. additionally, it doesn't seem right for the request to return a stacktrace. instead,
            // catching all exceptions and throwing a general exception here will result in a failed request with
            // a simple error message to indicate something went wrong during the file download.
            String errorMsg = "failed to download participant data";
            logger.error( errorMsg, e );
            throw new RuntimeException( errorMsg );
        }
    }

    @Override
    public Iterable<Map<String, Set<Object>>> getAllPreprocessedParticipantData(
            UUID studyId,
            UUID participatedInEntityKeyId,
            String token ) {
        return getParticipantDataHelper(
                studyId,
                participatedInEntityKeyId,
                RECORDED_BY_ENTITY_SET_NAME,
                PREPROCESSED_DATA_ENTITY_SET_NAME,
                token
        );
    }

    @Override
    public Iterable<Map<String, Set<Object>>> getAllParticipantData(
            UUID studyId,
            UUID participantEntityKeyId,
            String token ) {
        return getParticipantDataHelper(
                studyId,
                participantEntityKeyId,
                RECORDED_BY_ENTITY_SET_NAME,
                DATA_ENTITY_SET_NAME,
                token
        );
    }

    @Override
    public Iterable<Map<String, Set<Object>>> getAllParticipantAppsUsageData(
            UUID studyId,
            UUID participantEntityKeyId,
            String token ) {
        return getParticipantDataHelper(
                studyId,
                participantEntityKeyId,
                USED_BY_ENTITY_SET_NAME,
                CHRONICLE_USER_APPS,
                token
        );
    }

    @Override
    @Nonnull
    public Map<FullQualifiedName, Set<Object>> getParticipantEntity( UUID studyId, UUID participantEntityKeyId ) {

        try {
            ApiClient apiClient = apiClientCache.get( ApiClient.class );
            DataApi dataApi = apiClient.getDataApi();
            EntitySetsApi entitySetsApi = apiClient.getEntitySetsApi();

            String entitySetName = getParticipantEntitySetName( studyId );
            UUID entitySetId = entitySetsApi.getEntitySetId( entitySetName );
            if ( entitySetId == null ) {
                logger.error( "Unable to load participant EntitySet id." );
                return ImmutableMap.of();
            }

            Map<FullQualifiedName, Set<Object>> entity = dataApi.getEntity( entitySetId, participantEntityKeyId );
            if ( entity == null ) {
                logger.error( "Unable to get participant entity." );
                return ImmutableMap.of();
            }
            return entity;
        } catch ( ExecutionException e ) {
            logger.error( "Unable to get participant entity.", e );
            return ImmutableMap.of();
        }
    }

    @Override
    public ParticipationStatus getParticipationStatus( UUID studyId, String participantId ) {

        EntitySetsApi entitySetsApi;
        SearchApi searchApi;

        try {
            ApiClient apiClient = apiClientCache.get( ApiClient.class );
            entitySetsApi = apiClient.getEntitySetsApi();
            searchApi = apiClient.getSearchApi();
        } catch ( ExecutionException e ) {
            logger.error( "Unable to load apis." );
            return ParticipationStatus.UNKNOWN;
        }

        String participantsEntitySetName = getParticipantEntitySetName( studyId );
        UUID participantsEntitySetId = entitySetsApi.getEntitySetId( participantsEntitySetName );
        if ( participantsEntitySetId == null ) {
            logger.error( "unable to get the participants EntitySet id for studyId = {}", studyId );
            return ParticipationStatus.UNKNOWN;
        }

        UUID participantEntityKeyId = getParticipantEntityKeyId( participantId, studyId );
        if ( participantEntityKeyId == null ) {
            logger.error( "unable to load participant entity key id for participantId = {}", participantId );
            return ParticipationStatus.UNKNOWN;
        }

        Map<UUID, List<NeighborEntityDetails>> neighborResults = searchApi.executeFilteredEntityNeighborSearch(
                participantsEntitySetId,
                new EntityNeighborsFilter(
                        ImmutableSet.of( participantEntityKeyId ),
                        java.util.Optional.of( ImmutableSet.of() ),
                        java.util.Optional.of( ImmutableSet.of( entitySetIdMap.get( STUDY_ENTITY_SET_NAME ) ) ),
                        java.util.Optional.of( ImmutableSet.of( entitySetIdMap.get( PARTICIPATED_IN_AESN ) ) )
                )
        );

        Set<Map<FullQualifiedName, Set<Object>>> target = neighborResults
                .getOrDefault( participantEntityKeyId, ImmutableList.of() )
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
                Map<FullQualifiedName, Set<Object>> data = target.iterator().next();
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

    @Override
    public ChronicleQuestionnaire getQuestionnaire( UUID studyId, UUID questionnaireEKID ) {
        try {
            logger.info( "Retrieving questionnaire: studyId = {}, questionnaire EKID = {}",
                    studyId,
                    questionnaireEKID );

            UUID studyEKID = Preconditions.checkNotNull( getStudyEntityKeyId( studyId ), "invalid study: " + studyId );

            // get apis
            ApiClient apiClient = apiClientCache.get( ApiClient.class );
            SearchApi searchApi = apiClient.getSearchApi();

            // Get questionnaires that neighboring study
            Map<UUID, List<NeighborEntityDetails>> neighbors = searchApi.executeFilteredEntityNeighborSearch(
                    entitySetIdMap.get( STUDY_ENTITY_SET_NAME ),
                    new EntityNeighborsFilter(
                            Set.of( studyEKID ),
                            java.util.Optional.of( Set.of( entitySetIdMap.get( QUESTIONNAIRE_ENTITY_SET_NAME ) ) ),
                            java.util.Optional.of( Set.of( entitySetIdMap.get( STUDY_ENTITY_SET_NAME ) ) ),
                            java.util.Optional.of( Set.of( entitySetIdMap.get( PART_OF_ENTITY_SET_NAME ) ) )
                    )
            );

            // find questionnaire entity matching given entity key id
            if ( neighbors.containsKey( studyEKID ) ) {
                ChronicleQuestionnaire questionnaire = new ChronicleQuestionnaire();

                neighbors.get( studyEKID )
                        .stream()
                        .filter( neighbor -> neighbor.getNeighborDetails().isPresent() && neighbor.getNeighborId()
                                .isPresent() )
                        .filter( neighbor -> neighbor.getNeighborId().get().toString()
                                .equals( questionnaireEKID.toString() ) )
                        .map( neighbor -> neighbor.getNeighborDetails().get() )
                        .findFirst() // If a study has multiple questionnaires, we are only interested in the one with a matching EKID
                        .ifPresent( questionnaire::setQuestionnaireDetails );

                if ( questionnaire.getQuestionnaireDetails() == null ) {
                    logger.info( "questionnaire does not exist - studyId: {}, questionnaireEKID: {}, neighbors: {}",
                            studyId,
                            questionnaireEKID,
                            neighbors.size() );
                    throw new IllegalArgumentException(
                            "questionnaire does not exist, studyId: " + studyId + "questionnaire EKID = "
                                    + questionnaireEKID );
                }
                logger.info( "retrieved questionnaire: {}", questionnaire.getQuestionnaireDetails().toString() );

                // get questions neighboring questionnaire
                neighbors = searchApi.executeFilteredEntityNeighborSearch(
                        entitySetIdMap.get( QUESTIONNAIRE_ENTITY_SET_NAME ),
                        new EntityNeighborsFilter(
                                Set.of( questionnaireEKID ),
                                java.util.Optional.of( Set.of( entitySetIdMap.get( QUESTIONS_ENTITY_SET_NAME ) ) ),
                                java.util.Optional.of( Set.of( entitySetIdMap.get( QUESTIONNAIRE_ENTITY_SET_NAME ) ) ),
                                java.util.Optional.of( Set.of( entitySetIdMap.get( PART_OF_ENTITY_SET_NAME ) ) )
                        )
                );

                List<Map<FullQualifiedName, Set<Object>>> questions = neighbors
                        .getOrDefault( questionnaireEKID, List.of() )
                        .stream()
                        .filter( neighbor -> neighbor.getNeighborDetails().isPresent() )
                        .map( neighbor -> neighbor.getNeighborDetails().get() )
                        .collect( Collectors.toList() );

                questionnaire.setQuestions( questions );

                logger.info( "retrieved {} questions associated with questionnaire {}",
                        questions.size(),
                        questionnaireEKID );

                return questionnaire;
            }

        } catch ( Exception e ) {
            // catch all errors encountered during execution
            logger.error( "unable to retrieve questionnaire: studyId = {}, questionnaire = {}",
                    studyId,
                    questionnaireEKID );
            throw new RuntimeException( "questionnaire not found" );
        }

        /*
         * IF we get to this point, the requested questionnaire was not found. We shouldn't return null since
         * the caller would get an "ok" response. Instead send an error response.
         */
        throw new IllegalArgumentException( "questionnaire not found" );
    }

    public Map<UUID, Map<FullQualifiedName, Set<Object>>> getStudyQuestionnaires( UUID studyId ) {
        try {
            logger.info( "Retrieving questionnaires for study :{}", studyId );

            // check if study is valid
            UUID studyEntityKeyId = Preconditions
                    .checkNotNull( getStudyEntityKeyId( studyId ), "invalid studyId: " + studyId );

            // load apis
            ApiClient apiClient = apiClientCache.get( ApiClient.class );
            SearchApi searchApi = apiClient.getSearchApi();

            // filtered search on questionnaires ES to get neighbors of study
            Map<UUID, List<NeighborEntityDetails>> neighbors = searchApi
                    .executeFilteredEntityNeighborSearch(
                            entitySetIdMap.get( STUDY_ENTITY_SET_NAME ),
                            new EntityNeighborsFilter(
                                    Set.of( studyEntityKeyId ),
                                    java.util.Optional
                                            .of( Set.of( entitySetIdMap.get( QUESTIONNAIRE_ENTITY_SET_NAME ) ) ),
                                    java.util.Optional.of( Set.of( entitySetIdMap.get( STUDY_ENTITY_SET_NAME ) ) ),
                                    java.util.Optional.of( Set.of( entitySetIdMap.get( PART_OF_ENTITY_SET_NAME ) ) )
                            )
                    );

            // create a mapping from entity key id -> entity details
            List<NeighborEntityDetails> studyQuestionnaires = neighbors.getOrDefault( studyEntityKeyId, List.of() );
            Map<UUID, Map<FullQualifiedName, Set<Object>>> result = studyQuestionnaires
                    .stream()
                    .filter( neighbor -> neighbor.getNeighborId().isPresent() && neighbor.getNeighborDetails()
                            .isPresent() )
                    .collect( Collectors.toMap(
                            neighbor -> neighbor.getNeighborId().get(),
                            neighbor -> neighbor.getNeighborDetails().get()
                    ) );

            logger.info( "found {} questionnaires for study {}", result.size(), studyId );
            return result;

        } catch ( Exception e ) {
            logger.error( "failed to get questionnaires for study {}", studyId, e );
            throw new RuntimeException( "failed to get questionnaires" );
        }
    }

    @Override
    public void submitQuestionnaire(
            UUID studyId,
            String participantId,
            Map<UUID, Map<FullQualifiedName, Set<Object>>> questionnaireResponses ) {
        DataApi dataApi;
        EntitySetsApi entitySetsApi;
        try {
            logger.info( "submitting questionnaire: studyId = {}, participantId = {}", studyId, participantId );

            ApiClient apiClient = apiClientCache.get( ApiClient.class );
            dataApi = apiClient.getDataApi();
            entitySetsApi = apiClient.getEntitySetsApi();

            UUID participantEKID = Preconditions
                    .checkNotNull( getParticipantEntityKeyId( participantId, studyId ), "participant not found" );

            String participantESName = getParticipantEntitySetName( studyId );
            UUID participantESID = Preconditions
                    .checkNotNull( entitySetsApi.getEntitySetId( participantESName ),
                            "participant entity set does not exist" );

            ListMultimap<UUID, Map<UUID, Set<Object>>> entities = ArrayListMultimap.create();
            ListMultimap<UUID, DataAssociation> associations = ArrayListMultimap.create();

            OffsetDateTime dateTime = OffsetDateTime.now();

            List<UUID> questionEntityKeyIds = new ArrayList<>( questionnaireResponses.keySet() );
            for ( int i = 0; i < questionEntityKeyIds.size(); i++ ) {
                UUID questionEntityKeyId = questionEntityKeyIds.get( i );

                Map<UUID, Set<Object>> answerEntity = ImmutableMap.of(
                        propertyTypeIdsByFQN.get( VALUES_FQN ),
                        questionnaireResponses.get( questionEntityKeyId ).get( VALUES_FQN ) );
                entities.put( entitySetIdMap.get( ANSWERS_ENTITY_SET_NAME ), answerEntity );

                // 1. create participant -> respondsWith -> answer association
                Map<UUID, Set<Object>> respondsWithEntity = ImmutableMap.of(
                        propertyTypeIdsByFQN.get( DATE_TIME_FQN ),
                        ImmutableSet.of( dateTime )
                );
                associations.put( entitySetIdMap.get( RESPONDS_WITH_ENTITY_SET_NAME ), new DataAssociation(
                        participantESID,
                        java.util.Optional.empty(),
                        java.util.Optional.of( participantEKID ),
                        entitySetIdMap.get( ANSWERS_ENTITY_SET_NAME ),
                        java.util.Optional.of( i ),
                        java.util.Optional.empty(),
                        respondsWithEntity
                ) );

                // 2. create answer -> addresses -> question association
                Map<UUID, Set<Object>> addressesEntity = ImmutableMap.of(
                        propertyTypeIdsByFQN.get( COMPLETED_DATE_TIME_FQN ),
                        ImmutableSet.of( dateTime )
                );
                associations.put( entitySetIdMap.get( ADDRESSES_ENTITY_SET_NAME ), new DataAssociation(
                        entitySetIdMap.get( ANSWERS_ENTITY_SET_NAME ),
                        java.util.Optional.of( i ),
                        java.util.Optional.empty(),
                        entitySetIdMap.get( QUESTIONS_ENTITY_SET_NAME ),
                        java.util.Optional.empty(),
                        java.util.Optional.of( questionEntityKeyId ),
                        addressesEntity
                ) );
            }
            DataGraph dataGraph = new DataGraph( entities, associations );
            dataApi.createEntityAndAssociationData( dataGraph );

            logger.info( "submitted questionnaire: studyId = {}, participantId = {}", studyId, participantId );
        } catch ( Exception e ) {
            String errorMsg = "an error occurred while attempting to submit questionnaire";
            logger.error( errorMsg, e );
            throw new RuntimeException( errorMsg );
        }
    }
}
