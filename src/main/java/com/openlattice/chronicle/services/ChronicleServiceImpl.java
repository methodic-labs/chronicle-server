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
import com.openlattice.chronicle.ChronicleServerUtil;
import com.openlattice.chronicle.configuration.ChronicleConfiguration;
import com.openlattice.chronicle.constants.RecordType;
import com.openlattice.chronicle.data.ChronicleAppsUsageDetails;
import com.openlattice.chronicle.data.ChronicleQuestionnaire;
import com.openlattice.chronicle.data.ParticipationStatus;
import com.openlattice.chronicle.sources.AndroidDevice;
import com.openlattice.chronicle.sources.Datasource;
import com.openlattice.client.ApiClient;
import com.openlattice.data.*;
import com.openlattice.data.requests.FileType;
import com.openlattice.data.requests.NeighborEntityDetails;
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

    private final UUID userAppsDictESID = UUID.fromString( "628ad697-7ec8-4954-81d4-d5eab40001d9" );

    private final String username;
    private final String password;

    private final EventBus  eventBus;
    private final String    SEARCH_PREFIX = "entity";
    private final Set<UUID> dataKey;

    private final UUID studyESID;
    private final UUID metadataESID;
    private final UUID deviceESID;
    private final UUID dataESID;
    private final UUID hasESID;
    private final UUID preProcessedESID;
    private final UUID recordedByESID;
    private final UUID notificationsESID;
    private final UUID partOfESID;
    private final UUID usedByESID;
    private final UUID participatedInESID;
    private final UUID questionnaireESID;
    private final UUID questionsESID;
    private final UUID answersESID;
    private final UUID addressesESID;
    private final UUID respondsWithESID;
    private final UUID stringIdPTID;
    private final UUID personIdPSID;
    private final UUID dateLoggedPTID;
    private final UUID timezonePTID;
    private final UUID dateTimePTID;
    private final UUID versionPTID;
    private final UUID modelPTID;
    private final UUID userAppsESID;
    private final UUID titlePTID;
    private final UUID fullNamePTID;
    private final UUID recordTypePTID;
    private final UUID startDateTimePTID;
    private final UUID endDateTimePTID;
    private final UUID recordedDateTimePTID;
    private final UUID durationPTID;
    private final UUID olIdPTID;

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
        EntitySetsApi entitySetsApi = apiClient.getEntitySetsApi();

        studyESID = entitySetsApi.getEntitySetId( STUDY_ENTITY_SET_NAME );
        metadataESID = entitySetsApi.getEntitySetId( METADATA_ENTITY_SET_NAME );
        deviceESID = entitySetsApi.getEntitySetId( DEVICES_ENTITY_SET_NAME );
        dataESID = entitySetsApi.getEntitySetId( DATA_ENTITY_SET_NAME );
        hasESID = entitySetsApi.getEntitySetId( HAS_ENTITY_SET_NAME );
        preProcessedESID = entitySetsApi.getEntitySetId( PREPROCESSED_DATA_ENTITY_SET_NAME );
        recordedByESID = entitySetsApi.getEntitySetId( RECORDED_BY_ENTITY_SET_NAME );
        usedByESID = entitySetsApi.getEntitySetId( USED_BY_ENTITY_SET_NAME );
        participatedInESID = entitySetsApi.getEntitySetId( PARTICIPATED_IN_AESN );
        userAppsESID = entitySetsApi.getEntitySetId( CHRONICLE_USER_APPS );
        questionnaireESID = entitySetsApi.getEntitySetId( QUESTIONNAIRE_ENTITY_SET_NAME );
        questionsESID = entitySetsApi.getEntitySetId( QUESTIONS_ENTITY_SET_NAME );
        answersESID = entitySetsApi.getEntitySetId( ANSWERS_ENTITY_SET_NAME );
        addressesESID = entitySetsApi.getEntitySetId( ADDRESSES_ENTITY_SET_NAME );
        respondsWithESID = entitySetsApi.getEntitySetId( RESPONDS_WITH_ENTITY_SET_NAME );
        notificationsESID = entitySetsApi.getEntitySetId( NOTIFICATION_ENTITY_SET_NAME );
        partOfESID = entitySetsApi.getEntitySetId( PART_OF_ENTITY_SET_NAME );

        durationPTID = edmApi.getPropertyTypeId( DURATION.getNamespace(), DURATION.getName() );
        stringIdPTID = edmApi.getPropertyTypeId( STRING_ID_FQN.getNamespace(), STRING_ID_FQN.getName() );
        personIdPSID = edmApi.getPropertyTypeId( PERSON_ID_FQN.getNamespace(), PERSON_ID_FQN.getName() );
        dateLoggedPTID = edmApi.getPropertyTypeId( DATE_LOGGED_FQN.getNamespace(), DATE_LOGGED_FQN.getName() );
        versionPTID = edmApi.getPropertyTypeId( VERSION_FQN.getNamespace(), VERSION_FQN.getName() );
        olIdPTID = edmApi.getPropertyTypeId( OL_ID_FQN.getNamespace(), OL_ID_FQN.getName() );
        modelPTID = edmApi.getPropertyTypeId( MODEL_FQN.getNamespace(), MODEL_FQN.getName() );
        dateTimePTID = edmApi.getPropertyTypeId( DATE_TIME_FQN.getNamespace(), DATE_TIME_FQN.getName() );
        titlePTID = edmApi.getPropertyTypeId( TITLE_FQN.getNamespace(), TITLE_FQN.getName() );
        fullNamePTID = edmApi.getPropertyTypeId( FULL_NAME_FQN.getNamespace(), FULL_NAME_FQN.getName() );
        timezonePTID = edmApi.getPropertyTypeId( TIMEZONE_FQN.getNamespace(), TIMEZONE_FQN.getName() );
        recordTypePTID = edmApi.getPropertyTypeId( RECORD_TYPE_FQN.getNamespace(), RECORD_TYPE_FQN.getName() );
        startDateTimePTID = edmApi.getPropertyTypeId( START_DATE_TIME.getNamespace(), START_DATE_TIME.getName() );
        endDateTimePTID = edmApi.getPropertyTypeId( END_DATE_TIME.getNamespace(), END_DATE_TIME.getName() );
        recordedDateTimePTID = edmApi
                .getPropertyTypeId( RECORDED_DATE_TIME_FQN.getNamespace(), RECORDED_DATE_TIME_FQN.getName() );
        dataKey = edmApi.getEntityType( entitySetsApi.getEntitySet( dataESID ).getEntityTypeId() ).getKey();

        refreshStudyInformation();
        refreshUserAppsDictionary();

    }

    private UUID reserveEntityKeyId(
            UUID entitySetId,
            List<UUID> keyPropertyTypeIds,
            Map<UUID, Set<Object>> data,
            DataIntegrationApi dataIntegrationApi ) {

        return dataIntegrationApi.getEntityKeyIds( ImmutableSet.of( new EntityKey(
                entitySetId,
                ApiUtil.generateDefaultEntityId( keyPropertyTypeIds, data )
        ) ) ).iterator().next();
    }

    private UUID reserveDeviceEntityKeyId(
            Map<UUID, Set<Object>> data,
            DataIntegrationApi dataIntegrationApi ) {

        return reserveEntityKeyId(
                deviceESID,
                ImmutableList.of( stringIdPTID ),
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
                ChronicleServerUtil.getParticipantEntitySetName( studyId )
        );

    }

    private UUID getParticipantEntityKeyId( String participantId, UUID studyId ) {
        if ( studyParticipants.containsKey( studyId ) ) {
            Map<String, UUID> participantIdToEKMap = studyParticipants.get( studyId );

            if ( participantIdToEKMap.containsKey( participantId ) ) {
                return participantIdToEKMap.get( participantId );
            } else {
                logger.error( "Unable to get participantEntityKeyId. participant {} not associated with studId {} ",
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
        data.put( fullNamePTID, ImmutableSet.of( appPackageName ) );
        data.put( personIdPSID, Sets.newHashSet( participantId ) );

        return reserveEntityKeyId(
                usedByESID,
                ImmutableList.of( fullNamePTID, dateTimePTID, personIdPSID ),
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
        data.put( fullNamePTID, Sets.newHashSet( appPackageName ) );

        return reserveEntityKeyId(
                recordedByESID,
                ImmutableList.of( dateLoggedPTID, stringIdPTID, fullNamePTID ),
                data,
                dataIntegrationApi
        );
    }

    private UUID reserveUserAppEntityKeyId(
            Map<UUID, Set<Object>> entityData,
            DataIntegrationApi dataIntegrationApi ) {

        return reserveEntityKeyId(
                userAppsESID,
                ImmutableList.of( fullNamePTID ),
                entityData,
                dataIntegrationApi
        );
    }

    private UUID reserveMetadataEntityKeyId(
            Map<UUID, Set<Object>> entityData,
            DataIntegrationApi dataIntegrationApi ) {

        return reserveEntityKeyId(
                metadataESID,
                ImmutableList.of( olIdPTID ),
                entityData,
                dataIntegrationApi
        );
    }

    private UUID reserveHasEntityKeyId(
            Map<UUID, Set<Object>> entityData,
            DataIntegrationApi dataIntegrationApi ) {

        return reserveEntityKeyId(
                hasESID,
                ImmutableList.of( olIdPTID ),
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

                String appPackageName = appEntity.get( fullNamePTID ).iterator().next().toString();
                String appName = userAppsDict.get( appPackageName );
                if ( appName == null )
                    continue;
                String dateLogged = getMidnightDateTime( appEntity.get( dateLoggedPTID ).iterator().next()
                        .toString() );

                // create entity in chronicle_user_apps
                Map<UUID, Set<Object>> userAppEntityData = new HashMap<>();
                userAppEntityData.put( fullNamePTID, Sets.newHashSet( appPackageName ) );
                userAppEntityData.put( titlePTID, Sets.newHashSet( appName ) );

                UUID userAppEntityKeyId = reserveUserAppEntityKeyId( userAppEntityData, dataIntegrationApi );
                dataApi.updateEntitiesInEntitySet( userAppsESID,
                        ImmutableMap.of( userAppEntityKeyId, userAppEntityData ),
                        UpdateType.Merge );

                // association: chronicle_user_apps => chronicle_recorded_by => chronicle_device
                Map<UUID, Set<Object>> recordedByEntityData = new HashMap<>();
                recordedByEntityData.put( dateLoggedPTID, ImmutableSet.of( dateLogged ) );
                recordedByEntityData.put( stringIdPTID, ImmutableSet.of( deviceId ) );

                UUID recordedByEntityKeyId = reserveRecordedByEntityKeyId( recordedByEntityData,
                        appPackageName,
                        dataIntegrationApi );
                dataApi.updateEntitiesInEntitySet( recordedByESID,
                        ImmutableMap.of( recordedByEntityKeyId, recordedByEntityData ),
                        UpdateType.Merge );

                EntityDataKey src = new EntityDataKey( userAppsESID, userAppEntityKeyId );
                EntityDataKey dst = new EntityDataKey( deviceESID, deviceEntityKeyId );
                EntityDataKey edge = new EntityDataKey( recordedByESID, recordedByEntityKeyId );

                dataEdgeKeys.add( new DataEdgeKey( src, dst, edge ) );

                // association: chronicle_user_apps => chronicle_used_by => chronicle_participants_{studyId}
                Map<UUID, Set<Object>> usedByEntityData = new HashMap<>();
                usedByEntityData.put( dateTimePTID, ImmutableSet.of( dateLogged ) );

                UUID usedByEntityKeyId = reserveUsedByEntityKeyId( usedByEntityData,
                        appPackageName,
                        participantId,
                        dataIntegrationApi );
                dataApi.updateEntitiesInEntitySet( usedByESID,
                        ImmutableMap.of( usedByEntityKeyId, usedByEntityData ),
                        UpdateType.Merge );

                dst = new EntityDataKey( participantEntitySetId, participantEntityKeyId );
                edge = new EntityDataKey( usedByESID, usedByEntityKeyId );
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

        Set<OffsetDateTime> pushedDateTimes = data.stream()
                .map( entity -> {
                    ZoneId tz = ZoneId.of( "UTC" );
                    Iterator<Object> timezoneStringIterator = entity.get( timezonePTID ).iterator();
                    if ( timezoneStringIterator.hasNext() ) {
                        tz = ZoneId.of( timezoneStringIterator.next().toString() );
                    }

                    HashSet<OffsetDateTime> dateTimes = new HashSet<>();
                    for ( Object date : entity.get( dateLoggedPTID ) ) {
                        dateTimes.add(
                                OffsetDateTime
                                        .parse( date.toString() )
                                        .toInstant()
                                        .atZone( tz )
                                        .toOffsetDateTime() );

                    }
                    return dateTimes;
                } )
                .flatMap( Set::stream )
                .filter( datetime -> datetime.isAfter( MINIMUM_DATE ) )
                .collect( Collectors.toSet() );

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
        metadataEntityData.put( olIdPTID, Set.of( participantEntityKeyId ) );
        UUID metadataEntityKeyId = reserveMetadataEntityKeyId( metadataEntityData, dataIntegrationApi );

        Map<FullQualifiedName, Set<Object>> entity = dataApi.getEntity( metadataESID, metadataEntityKeyId );
        metadataEntityData.put( startDateTimePTID, entity.getOrDefault( START_DATE_TIME, Set.of( firstDateTime ) ) );
        metadataEntityData.put( endDateTimePTID, Set.of( lastDateTime ) );
        uniqueDates.addAll( entity.getOrDefault( RECORDED_DATE_TIME_FQN, Set.of() ) );
        metadataEntityData.put( recordedDateTimePTID, uniqueDates );

        dataApi.updateEntitiesInEntitySet( metadataESID,
                ImmutableMap.of( metadataEntityKeyId, metadataEntityData ),
                UpdateType.PartialReplace );

        Map<UUID, Set<Object>> hasEntityData = new HashMap<>();
        hasEntityData.put( olIdPTID, Set.of( firstDateTime ) );
        UUID hasEntityKeyId = reserveHasEntityKeyId( metadataEntityData, dataIntegrationApi );
        dataApi.updateEntitiesInEntitySet( hasESID,
                ImmutableMap.of( hasEntityKeyId, hasEntityData ),
                UpdateType.Merge );

        EntityDataKey dst = new EntityDataKey( metadataESID, metadataEntityKeyId );
        EntityDataKey edge = new EntityDataKey( hasESID, hasEntityKeyId );
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
            entities.put( dataESID, Multimaps.asMap( data.get( i ) ) );

            Map<UUID, Set<Object>> recordedByEntity = ImmutableMap
                    .of( dateLoggedPTID, Sets.newHashSet( timeStamp ) );

            associations.put( recordedByESID, new DataAssociation(
                    dataESID,
                    java.util.Optional.of( i ),
                    java.util.Optional.empty(),
                    deviceESID,
                    java.util.Optional.empty(),
                    java.util.Optional.of( deviceEntityKeyId ),
                    recordedByEntity
            ) );

            associations.put( recordedByESID, new DataAssociation(
                    dataESID,
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
            dataApi.updateEntitiesInEntitySet( usedByESID, associationData, UpdateType.Replace );
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
                        java.util.Optional.of( ImmutableSet.of( userAppsESID ) ),
                        java.util.Optional.of( ImmutableSet.of( participantEntitySetId ) ),
                        java.util.Optional.of( ImmutableSet.of( usedByESID ) )
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
        deviceData.put( stringIdPTID, Sets.newHashSet( datasourceId ) );

        if ( datasource.isPresent() && AndroidDevice.class.isAssignableFrom( datasource.get().getClass() ) ) {
            AndroidDevice device = (AndroidDevice) datasource.get();
            deviceData.put( modelPTID, Sets.newHashSet( device.getModel() ) );
            deviceData.put( versionPTID, Sets.newHashSet( device.getOsVersion() ) );
        }

        UUID deviceEntityKeyId = reserveDeviceEntityKeyId( deviceData, dataIntegrationApi );
        if ( deviceEntityKeyId == null ) {
            logger.error( "Unable to reserve deviceEntityKeyId, dataSourceId = {}, studyId = {}, participantId = {}",
                    datasourceId,
                    studyId,
                    participantId );
            return null;
        }
        dataApi.updateEntitiesInEntitySet( deviceESID,
                ImmutableMap.of( deviceEntityKeyId, deviceData ),
                UpdateType.Merge );

        EntityDataKey deviceEDK = new EntityDataKey( deviceESID, deviceEntityKeyId );
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
        EntityDataKey studyEDK = new EntityDataKey( studyESID, studyEntityKeyId );

        ListMultimap<UUID, DataEdge> associations = ArrayListMultimap.create();

        Map<UUID, Set<Object>> usedByEntity = ImmutableMap
                .of( stringIdPTID, Sets.newHashSet( UUID.randomUUID() ) );

        associations.put( usedByESID, new DataEdge( deviceEDK, participantEDK, usedByEntity ) );
        associations.put( usedByESID, new DataEdge( deviceEDK, studyEDK, usedByEntity ) );

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
                        studyESID,
                        new EntityNeighborsFilter(
                                studyEntityKeyIds,
                                java.util.Optional.of( ImmutableSet.of( notificationsESID ) ),
                                java.util.Optional.of( ImmutableSet.of( studyESID ) ),
                                java.util.Optional.of( ImmutableSet.of( partOfESID ) )
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
                userAppsDictESID,
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
                .executeEntitySetDataQuery( studyESID, new SearchTerm( "*", 0, SearchApi.MAX_SEARCH_RESULTS ) )
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
                studyESID,
                new EntityNeighborsFilter(
                        studyEntityKeyIds,
                        java.util.Optional.of( participantEntitySetIds ),
                        java.util.Optional.of( ImmutableSet.of() ),
                        java.util.Optional.of( ImmutableSet.of( participatedInESID ) )
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
                                        java.util.Optional.of( ImmutableSet.of( deviceESID ) ),
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

            String participantEntitySetName = ChronicleServerUtil.getParticipantEntitySetName( studyId );
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

            Map<FullQualifiedName, UUID> propertyTypeIdsByFQN = propertyTypesById
                    .values()
                    .stream()
                    .collect( Collectors.toMap( PropertyType::getType, PropertyType::getId ) );

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
                                    if (propertyType.getDatatype() == EdmPrimitiveTypeKind.DateTimeOffset) {
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
                                                    }
                                                    catch ( Exception e) {
                                                        return null;
                                                    }
                                                })
                                                .filter( StringUtils::isBlank )
                                                .collect( Collectors.toSet() );
                                        cleanEntityData.put( APP_PREFIX + propertyTitle, dateTimeValues );
                                    }
                                    else {
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

            String entitySetName = ChronicleServerUtil.getParticipantEntitySetName( studyId );
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

        String participantsEntitySetName = ChronicleServerUtil.getParticipantEntitySetName( studyId );
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
                        java.util.Optional.of( ImmutableSet.of( studyESID ) ),
                        java.util.Optional.of( ImmutableSet.of( participatedInESID ) )
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
                    studyESID,
                    new EntityNeighborsFilter(
                            Set.of( studyEKID ),
                            java.util.Optional.of( Set.of( questionnaireESID ) ),
                            java.util.Optional.of( Set.of( studyESID ) ),
                            java.util.Optional.of( Set.of( partOfESID ) )
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
                        questionnaireESID,
                        new EntityNeighborsFilter(
                                Set.of( questionnaireEKID ),
                                java.util.Optional.of( Set.of( questionsESID ) ),
                                java.util.Optional.of( Set.of( questionnaireESID ) ),
                                java.util.Optional.of( Set.of( partOfESID ) )
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
            throw new RuntimeException( "questionnaire not found");
        }

        /*
         * IF we get to this point, the requested questionnaire was not found. We shouldn't return null since
         * the caller would get an "ok" response. Instead send an error response.
         */
        throw new IllegalArgumentException( "questionnaire not found" );
    }
}
