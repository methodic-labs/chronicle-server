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
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.*;
import com.openlattice.ApiUtil;
import com.openlattice.apps.App;
import com.openlattice.apps.AppApi;
import com.openlattice.apps.UserAppConfig;
import com.openlattice.auth0.Auth0Delegate;
import com.openlattice.authentication.Auth0Configuration;
import com.openlattice.authorization.securable.AbstractSecurableObject;
import com.openlattice.chronicle.configuration.ChronicleConfiguration;
import com.openlattice.chronicle.constants.AppComponent;
import com.openlattice.chronicle.constants.CollectionTemplateTypeName;
import com.openlattice.chronicle.constants.RecordType;
import com.openlattice.chronicle.data.ChronicleAppsUsageDetails;
import com.openlattice.chronicle.data.ChronicleQuestionnaire;
import com.openlattice.chronicle.data.DeleteType;
import com.openlattice.chronicle.data.ParticipationStatus;
import com.openlattice.chronicle.sources.AndroidDevice;
import com.openlattice.chronicle.sources.Datasource;
import com.openlattice.client.ApiClient;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.collections.CollectionTemplateType;
import com.openlattice.collections.CollectionsApi;
import com.openlattice.collections.EntitySetCollection;
import com.openlattice.collections.EntityTypeCollection;
import com.openlattice.data.*;
import com.openlattice.data.requests.EntitySetSelection;
import com.openlattice.data.requests.FileType;
import com.openlattice.data.requests.NeighborEntityDetails;
import com.openlattice.data.requests.NeighborEntityIds;
import com.openlattice.directory.PrincipalApi;
import com.openlattice.edm.EdmApi;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.set.EntitySetPropertyMetadata;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.entitysets.EntitySetsApi;
import com.openlattice.retrofit.RhizomeRetrofitCallException;
import com.openlattice.search.SearchApi;
import com.openlattice.search.requests.EntityNeighborsFilter;
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
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.openlattice.chronicle.constants.AppComponent.*;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.*;
import static com.openlattice.chronicle.constants.EdmConstants.*;
import static com.openlattice.chronicle.constants.OutputConstants.*;
import static com.openlattice.edm.EdmConstants.ID_FQN;

public class ChronicleServiceImpl implements ChronicleService {
    protected static final Logger logger = LoggerFactory.getLogger( ChronicleServiceImpl.class );

    private final long ENTITY_SETS_REFRESH_INTERVAL = 15 * 60 * 1000; // 15 minutes
    private final long STUDY_INFO_REFRESH_INTERVAL  = 60 * 1000; // 1 minute

    // orgId -> studyId -> participantId -> EKID
    private final Map<UUID, Map<UUID, Map<String, UUID>>> studyParticipantsByOrg = Maps.newHashMap();

    // orgId -> studyId -> studyEKID
    private final Map<UUID, Map<UUID, UUID>> studyEntityKeyIdsByOrg = Maps.newHashMap();

    // orgId -> deviceId -> deviceEKID
    private final Map<UUID, Map<String, UUID>> deviceEntityKeyIdsByOrg = Maps.newHashMap();

    // appName -> orgId -> templateName -> entitySetID
    private Map<AppComponent, Map<UUID, Map<CollectionTemplateTypeName, UUID>>> entitySetIdsByOrgId;

    private final Set<String> systemAppPackageNames = Collections.synchronizedSet( new HashSet<>() );

    private final Map<UUID, PropertyType>      propertyTypesById;
    private final Map<FullQualifiedName, UUID> propertyTypeIdsByFQN;

    private final UUID appsDictionaryESID;

    private final String username;
    private final String password;

    private final transient LoadingCache<Class<?>, ApiClient> prodApiClientCache;
    private final transient LoadingCache<Class<?>, ApiClient> intApiClientCache;
    private final           Auth0Delegate                     auth0Client;

    public ChronicleServiceImpl(
            ChronicleConfiguration chronicleConfiguration,
            Auth0Configuration auth0Configuration
    ) throws ExecutionException {

        this.auth0Client = Auth0Delegate.fromConfig( auth0Configuration );
        this.username = chronicleConfiguration.getUser();
        this.password = chronicleConfiguration.getPassword();

        prodApiClientCache = CacheBuilder
                .newBuilder()
                .expireAfterWrite( 10, TimeUnit.HOURS )
                .build( new CacheLoader<Class<?>, ApiClient>() {
                    @Override
                    public ApiClient load( Class<?> key ) throws Exception {

                        String jwtToken = auth0Client.getIdToken( username, password );
                        return new ApiClient( RetrofitFactory.Environment.PRODUCTION, () -> jwtToken );
                    }
                } );

        ApiClient prodApiClient = prodApiClientCache.get( ApiClient.class );

        intApiClientCache = CacheBuilder
                .newBuilder()
                .expireAfterWrite( 10, TimeUnit.HOURS )
                .build( new CacheLoader<Class<?>, ApiClient>() {
                    @Override
                    public ApiClient load( Class<?> key ) throws Exception {

                        String jwtToken = auth0Client.getIdToken( username, password );
                        return new ApiClient( RetrofitFactory.Environment.PROD_INTEGRATION, () -> jwtToken );
                    }
                } );

        EdmApi edmApi = prodApiClient.getEdmApi();
        EntitySetsApi entitySetsApi = prodApiClient.getEntitySetsApi();

        initializeEntitySets();

        // get propertyTypeId map
        Iterable<PropertyType> propertyTypes = edmApi.getPropertyTypes();
        propertyTypesById = StreamSupport
                .stream( propertyTypes.spliterator(), false )
                .collect( ImmutableMap.toImmutableMap( PropertyType::getId, Function.identity() ) );
        propertyTypeIdsByFQN = propertyTypesById
                .values()
                .stream()
                .collect( ImmutableMap.toImmutableMap( PropertyType::getType, PropertyType::getId ) );

        appsDictionaryESID = entitySetsApi.getEntitySetId( CHRONICLE_APPLICATION_DICTIONARY );

        refreshUserAppsDictionary();
        refreshStudyInformation();
    }

    private String getFirstValueOrNull( Map<FullQualifiedName, Set<Object>> entity, FullQualifiedName fqn ) {
        if ( entity.getOrDefault( fqn, Set.of() ).isEmpty() ) {
            return null;
        }
        return entity.get( fqn ).iterator().next().toString();
    }

    private UUID getFirstUUIDOrNull( Map<FullQualifiedName, Set<Object>> entity, FullQualifiedName fqn ) {
        String firstValue = getFirstValueOrNull( entity, fqn );
        if ( firstValue == null ) {
            return null;
        }

        return UUID.fromString( firstValue );
    }

    @Scheduled( fixedRate = ENTITY_SETS_REFRESH_INTERVAL )
    private void initializeEntitySets() throws ExecutionException {

        ApiClient prodApiClient = prodApiClientCache.get( ApiClient.class );

        AppApi appApi = prodApiClient.getAppApi();
        CollectionsApi collectionsApi = prodApiClient.getCollectionsApi();

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

        entitySetIdsByOrgId = ImmutableMap.copyOf( entitySets );
    }

    private UUID getEntitySetId(
            UUID organizationId,
            AppComponent appComponent,
            CollectionTemplateTypeName templateName ) {

        Map<CollectionTemplateTypeName, UUID> templateEntitySetIdMap = entitySetIdsByOrgId
                .getOrDefault( appComponent, ImmutableMap.of() )
                .getOrDefault( organizationId, ImmutableMap.of() );

        if ( templateEntitySetIdMap.isEmpty() ) {
            logger.error( "organization {} does not have app {} installed", organizationId, appComponent );
        }

        if ( !templateEntitySetIdMap.containsKey( templateName ) ) {
            logger.error( "app {} does not have a template {} in its entityTypeCollection",
                    appComponent,
                    templateName );
        }

        return templateEntitySetIdMap.get( templateName );

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
            UUID devicesESID,
            Map<UUID, Set<Object>> data,
            DataIntegrationApi dataIntegrationApi ) {

        return reserveEntityKeyId(
                devicesESID,
                ImmutableList.of( propertyTypeIdsByFQN.get( STRING_ID_FQN ) ),
                data,
                dataIntegrationApi
        );
    }

    private UUID getStudyEntityKeyId( UUID organizationId, UUID studyId ) {
        logger.info( "Retrieving studyEKID: studyId = {}, orgId = {}", studyId, organizationId );

        return studyEntityKeyIdsByOrg.getOrDefault( organizationId, Map.of() ).getOrDefault( studyId, null );
    }

    private UUID getParticipantEntityKeyId( UUID organizationId, UUID studyId, String participantId ) {
        Map<UUID, Map<String, UUID>> studyParticipants = studyParticipantsByOrg
                .getOrDefault( organizationId, Map.of() );

        return studyParticipants.getOrDefault( studyId, Map.of() ).getOrDefault( participantId, null );
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
            UUID usedByESID,
            Map<UUID, Set<Object>> entityData,
            String appPackageName,
            String participantId,
            DataIntegrationApi dataIntegrationApi ) {

        // UUID usedByESID = Preconditions.checkNotNull( getEntitySetId(  ) )
        Map<UUID, Set<Object>> data = new HashMap<>( entityData );
        data.put( propertyTypeIdsByFQN.get( FULL_NAME_FQN ), ImmutableSet.of( appPackageName ) );
        data.put( propertyTypeIdsByFQN.get( PERSON_ID_FQN ), Sets.newHashSet( participantId ) );

        return reserveEntityKeyId(
                usedByESID,
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
            UUID recordedByESID,
            Map<UUID, Set<Object>> recordedByEntity,
            String appPackageName,
            DataIntegrationApi dataIntegrationApi ) {

        Map<UUID, Set<Object>> data = new HashMap<>( recordedByEntity );
        data.put( propertyTypeIdsByFQN.get( FULL_NAME_FQN ), Sets.newHashSet( appPackageName ) );

        return reserveEntityKeyId(
                recordedByESID,
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
            UUID entitySetId,
            Map<UUID, Set<Object>> entityData,
            DataIntegrationApi dataIntegrationApi ) {

        return reserveEntityKeyId(
                entitySetId,
                ImmutableList.of( propertyTypeIdsByFQN.get( FULL_NAME_FQN ) ),
                entityData,
                dataIntegrationApi
        );
    }

    private UUID reserveMetadataEntityKeyId(
            UUID entitySetId,
            Map<UUID, Set<Object>> entityData,
            DataIntegrationApi dataIntegrationApi ) {

        return reserveEntityKeyId(
                entitySetId,
                ImmutableList.of( propertyTypeIdsByFQN.get( OL_ID_FQN ) ),
                entityData,
                dataIntegrationApi
        );
    }

    private UUID reserveHasEntityKeyId(
            UUID entitySetId,
            Map<UUID, Set<Object>> entityData,
            DataIntegrationApi dataIntegrationApi ) {

        return reserveEntityKeyId(
                entitySetId,
                ImmutableList.of( propertyTypeIdsByFQN.get( OL_ID_FQN ) ),
                entityData,
                dataIntegrationApi
        );
    }

    private void createUserAppsEntitiesAndAssociations(
            DataApi dataApi,
            DataIntegrationApi dataIntegrationApi,
            List<SetMultimap<UUID, Object>> data,
            UUID organizationId,
            UUID deviceEntityKeyId,
            UUID participantEntitySetId,
            UUID participantEntityKeyId,
            String participantId,
            String deviceId ) {

        // entity set ids
        UUID userAppsESID = ensureEntitySetExists( organizationId, CHRONICLE_DATA_COLLECTION, USER_APPS );
        UUID usedByESID = ensureEntitySetExists( organizationId, CHRONICLE_DATA_COLLECTION, USED_BY );
        UUID recordedByESID = ensureEntitySetExists( organizationId, CHRONICLE_DATA_COLLECTION, RECORDED_BY );
        UUID devicesESID = ensureEntitySetExists( organizationId, CHRONICLE_DATA_COLLECTION, DEVICE );

        /*
         * Most of the data pushed by devices does not correspond to apps that were visible in the UI.
         * Here we will only record the apps that exist in the chronicle user apps dictionary
         *
         */

        int numAppsUploaded = 0;
        for ( SetMultimap<UUID, Object> appEntity : data ) {
            try {
                Set<DataEdgeKey> dataEdgeKeys = new HashSet<>();

                String appPackageName, appName;
                appPackageName = appName = appEntity.get( propertyTypeIdsByFQN.get( FULL_NAME_FQN ) ).iterator().next()
                        .toString();
                if ( systemAppPackageNames.contains( appPackageName ) )
                    continue; // a 'system' app

                if ( appEntity.containsKey( propertyTypeIdsByFQN.get( TITLE_FQN ) ) ) {
                    appName = appEntity.get( propertyTypeIdsByFQN.get( TITLE_FQN ) ).iterator().next().toString();
                }

                String dateLogged = getMidnightDateTime( appEntity.get( propertyTypeIdsByFQN.get( DATE_LOGGED_FQN ) )
                        .iterator().next()
                        .toString() );

                // create entity in chronicle_user_apps
                Map<UUID, Set<Object>> userAppEntityData = new HashMap<>();
                userAppEntityData.put( propertyTypeIdsByFQN.get( FULL_NAME_FQN ), Sets.newHashSet( appPackageName ) );
                userAppEntityData.put( propertyTypeIdsByFQN.get( TITLE_FQN ), Sets.newHashSet( appName ) );

                UUID userAppEntityKeyId = reserveUserAppEntityKeyId( userAppsESID,
                        userAppEntityData,
                        dataIntegrationApi );
                dataApi.updateEntitiesInEntitySet( userAppsESID,
                        ImmutableMap.of( userAppEntityKeyId, userAppEntityData ),
                        UpdateType.Merge );

                // association: chronicle_user_apps => chronicle_recorded_by => chronicle_device
                Map<UUID, Set<Object>> recordedByEntityData = new HashMap<>();
                recordedByEntityData.put( propertyTypeIdsByFQN.get( DATE_LOGGED_FQN ), ImmutableSet.of( dateLogged ) );
                recordedByEntityData.put( propertyTypeIdsByFQN.get( STRING_ID_FQN ), ImmutableSet.of( deviceId ) );

                UUID recordedByEntityKeyId = reserveRecordedByEntityKeyId(
                        recordedByESID,
                        recordedByEntityData,
                        appPackageName,
                        dataIntegrationApi
                );
                dataApi.updateEntitiesInEntitySet(
                        recordedByESID,
                        ImmutableMap.of( recordedByEntityKeyId, recordedByEntityData ),
                        UpdateType.Merge
                );

                EntityDataKey src = new EntityDataKey( userAppsESID, userAppEntityKeyId );
                EntityDataKey dst = new EntityDataKey(
                        devicesESID,
                        deviceEntityKeyId
                );
                EntityDataKey edge = new EntityDataKey(
                        recordedByESID,
                        recordedByEntityKeyId
                );

                dataEdgeKeys.add( new DataEdgeKey( src, dst, edge ) );

                // association: chronicle_user_apps => chronicle_used_by => chronicle_participants_{studyId}
                Map<UUID, Set<Object>> usedByEntityData = new HashMap<>();
                usedByEntityData.put( propertyTypeIdsByFQN.get( DATE_TIME_FQN ), ImmutableSet.of( dateLogged ) );

                UUID usedByEntityKeyId = reserveUsedByEntityKeyId(
                        usedByESID,
                        usedByEntityData,
                        appPackageName,
                        participantId,
                        dataIntegrationApi );
                dataApi.updateEntitiesInEntitySet(
                        usedByESID,
                        ImmutableMap.of( usedByEntityKeyId, usedByEntityData ),
                        UpdateType.Merge
                );

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

    /*
     * Creates or adds to an existing metadata entity, with general statistics (at this moment mostly datetimes)
     * about the data collection.
     */

    private void updateParticipantMetadata(
            DataApi dataApi,
            DataIntegrationApi dataIntegrationApi,
            List<SetMultimap<UUID, Object>> data,
            UUID organizationId,
            UUID participantEntitySetId,
            UUID participantEntityKeyId,
            String participantId ) {

        // get entity sets
        UUID metadataESID = ensureEntitySetExists( organizationId, CHRONICLE, METADATA );
        UUID hasESID = ensureEntitySetExists( organizationId, CHRONICLE, HAS );

        // get all dates in new data batch
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

        if ( pushedDateTimes.size() == 0 ) {
            return;
        }

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
        UUID metadataEntityKeyId = reserveMetadataEntityKeyId( metadataESID, metadataEntityData, dataIntegrationApi );

        // verify if there is already an entry of metadata for participant
        // error means there is no metadata yet.
        Map<FullQualifiedName, Set<Object>> entity = new HashMap<>();
        try {
            entity = dataApi
                    .getEntity( metadataESID, metadataEntityKeyId );
        } catch ( RhizomeRetrofitCallException e ) {}

        metadataEntityData.put( propertyTypeIdsByFQN.get( START_DATE_TIME_FQN ),
                entity.getOrDefault( START_DATE_TIME_FQN, Set.of( firstDateTime ) ) );
        metadataEntityData.put( propertyTypeIdsByFQN.get( END_DATE_TIME_FQN ), Set.of( lastDateTime ) );
        uniqueDates.addAll( entity.getOrDefault( RECORDED_DATE_TIME_FQN, Set.of() ) );
        metadataEntityData.put( propertyTypeIdsByFQN.get( RECORDED_DATE_TIME_FQN ), uniqueDates );

        dataApi.updateEntitiesInEntitySet( metadataESID,
                ImmutableMap.of( metadataEntityKeyId, metadataEntityData ),
                UpdateType.PartialReplace );

        Map<UUID, Set<Object>> hasEntityData = new HashMap<>();
        hasEntityData.put( propertyTypeIdsByFQN.get( OL_ID_FQN ), Set.of( firstDateTime ) );
        UUID hasEntityKeyId = reserveHasEntityKeyId( hasESID, metadataEntityData, dataIntegrationApi );
        dataApi.updateEntitiesInEntitySet( hasESID,
                ImmutableMap.of( hasEntityKeyId, hasEntityData ),
                UpdateType.PartialReplace );

        EntityDataKey dst = new EntityDataKey( metadataESID, metadataEntityKeyId );
        EntityDataKey edge = new EntityDataKey( hasESID, hasEntityKeyId );
        EntityDataKey src = new EntityDataKey( participantEntitySetId, participantEntityKeyId );
        DataEdgeKey dataEdgeKey = new DataEdgeKey( src, dst, edge );
        dataApi.createEdges( Set.of( dataEdgeKey ) );

        logger.info( "Uploaded user metadata entries: participantId = {}", participantId );
    }

    private UUID ensureEntitySetExists(
            UUID organizationId,
            AppComponent appComponent,
            CollectionTemplateTypeName template ) {
        return Preconditions.checkNotNull( getEntitySetId( organizationId, appComponent, template ),
                appComponent + " does not exist in org " + organizationId );
    }

    private void createAppDataEntitiesAndAssociations(
            DataApi dataApi,
            List<SetMultimap<UUID, Object>> data,
            UUID organizationId,
            UUID deviceEntityKeyId,
            String participantId,
            UUID participantEntityKeyId,
            UUID participantEntitySetId ) {

        // entity set ids
        UUID appDataESID = ensureEntitySetExists( organizationId, CHRONICLE_DATA_COLLECTION, APPDATA );
        UUID recordedByESID = ensureEntitySetExists( organizationId, CHRONICLE_DATA_COLLECTION, RECORDED_BY );
        UUID devicesESID = ensureEntitySetExists( organizationId, CHRONICLE_DATA_COLLECTION, DEVICE );

        ListMultimap<UUID, Map<UUID, Set<Object>>> entities = ArrayListMultimap.create();
        ListMultimap<UUID, DataAssociation> associations = ArrayListMultimap.create();

        OffsetDateTime timeStamp = OffsetDateTime.now();

        for ( int i = 0; i < data.size(); i++ ) {
            entities.put( appDataESID, Multimaps.asMap( data.get( i ) ) );

            Map<UUID, Set<Object>> recordedByEntity = ImmutableMap
                    .of( propertyTypeIdsByFQN.get( DATE_LOGGED_FQN ), Sets.newHashSet( timeStamp ) );

            associations.put( recordedByESID, new DataAssociation(
                    appDataESID,
                    Optional.of( i ),
                    Optional.empty(),
                    devicesESID,
                    Optional.empty(),
                    Optional.of( deviceEntityKeyId ),
                    recordedByEntity
            ) );

            associations.put( recordedByESID, new DataAssociation(
                    appDataESID,
                    Optional.of( i ),
                    Optional.empty(),
                    participantEntitySetId,
                    Optional.empty(),
                    Optional.of( participantEntityKeyId ),
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
            UUID organizationId,
            UUID studyId,
            String participantId,
            Map<UUID, Map<FullQualifiedName, Set<Object>>> associationDetails ) {

        logger.info( "Updating apps usage associations: participantId = {}, studyId = {}", participantId, studyId );

        DataApi dataApi;
        EdmApi edmApi;
        try {
            ApiClient apiClient = prodApiClientCache.get( ApiClient.class );
            dataApi = apiClient.getDataApi();
            edmApi = apiClient.getEdmApi();

            // participant must exist
            boolean knownParticipant = isKnownParticipant( organizationId, studyId, participantId );
            if ( !knownParticipant ) {
                logger.error( "unable to update apps usage association data because unknown participant = {}",
                        participantId );
                throw new IllegalArgumentException( "participant not found" );
            }

            // get entity set ids
            UUID usedByESID = Preconditions.checkNotNull( getEntitySetId( organizationId,
                    CHRONICLE_DATA_COLLECTION, USED_BY ),
                    "usedBy entity does not exist" );

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
            dataApi.updateEntitiesInEntitySet( usedByESID,
                    associationData,
                    UpdateType.Replace );

            logger.info( "updated {} apps usage associations", associationDetails.size() );
            return associationDetails.size();

        } catch ( Exception e ) {
            String error = "error updating apps usage: participant = " + participantId + ", studyId = " + studyId
                    + ", orgID = {}" + organizationId;
            logger.error( error, e );
            throw new IllegalStateException( error );
        }
    }

    // return a list of all the apps used by a participant filtered by the current date
    @Override
    public List<ChronicleAppsUsageDetails> getParticipantAppsUsageData(
            UUID organizationId,
            UUID studyId,
            String participantId,
            String date ) {

        logger.info( "Retrieving user apps: participantId = {}, studyId = {}, orgId: {}",
                participantId,
                studyId,
                organizationId );

        SearchApi searchApi;
        try {
            ApiClient apiClient = intApiClientCache.get( ApiClient.class );
            searchApi = apiClient.getSearchApi();

            // date must be valid.
            LocalDate.parse( date ); // this will throw a DateTimeParseException if date cannot be parsed

            // participant must exist
            UUID participantEKID = Preconditions
                    .checkNotNull( getParticipantEntityKeyId( organizationId, studyId, participantId ),
                            "participant does not exist" );

            // get entity set ids
            UUID participantsESID = ensureEntitySetExists( organizationId, CHRONICLE, PARTICIPANTS );
            UUID userAppsESID = ensureEntitySetExists( organizationId, CHRONICLE_DATA_COLLECTION, USER_APPS );
            UUID usedByESID = ensureEntitySetExists( organizationId, CHRONICLE_DATA_COLLECTION, USED_BY );

            // search participants to retrieve neighbors in user apps entity set
            Map<UUID, List<NeighborEntityDetails>> participantNeighbors = searchApi.executeFilteredEntityNeighborSearch(
                    participantsESID,
                    new EntityNeighborsFilter(
                            ImmutableSet.of( participantEKID ),
                            Optional.of( ImmutableSet.of( userAppsESID ) ),
                            Optional.of( ImmutableSet.of( participantsESID ) ),
                            Optional.of( ImmutableSet.of( usedByESID ) )
                    )
            );

            if ( participantNeighbors.containsKey( participantEKID ) ) {
                return participantNeighbors.get( participantEKID )
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
        } catch ( ExecutionException e ) {
            logger.error( "error retrieving user apps: participant = {}, studyId = {}, orgId = {}",
                    participantId,
                    studyId,
                    organizationId );
            throw new IllegalStateException( e );
        }

    }

    //  TODO: add in throws exception!
    @Override
    public Integer logData(
            UUID organizationId,
            UUID studyId,
            String participantId,
            String deviceId,
            List<SetMultimap<UUID, Object>> data ) {

        DataApi dataApi;
        DataIntegrationApi dataIntegrationApi;
        try {
            ApiClient apiClient = intApiClientCache.get( ApiClient.class );
            dataApi = apiClient.getDataApi();
            dataIntegrationApi = apiClient.getDataIntegrationApi();
        } catch ( ExecutionException e ) {
            logger.error( "Unable to load apis." );
            return 0;
        }

        UUID participantEntityKeyId = getParticipantEntityKeyId( organizationId, studyId, participantId );
        if ( participantEntityKeyId == null ) {
            logger.error( "Unable to retrieve participantEntityKeyId, studyId = {}, participantId = {}",
                    studyId,
                    participantId );
            return 0;
        }

        ParticipationStatus status = getParticipationStatus( organizationId, studyId, participantId );
        if ( ParticipationStatus.NOT_ENROLLED.equals( status ) ) {
            logger.warn( "participantId = {} is not enrolled, ignoring data upload", participantId );
            return 0;
        }

        UUID participantEntitySetId = ensureEntitySetExists( organizationId, CHRONICLE, PARTICIPANTS );
        UUID deviceEntityKeyId = getDeviceEntityKeyId( organizationId, studyId, participantId, deviceId );

        createUserAppsEntitiesAndAssociations( dataApi,
                dataIntegrationApi,
                data,
                organizationId,
                deviceEntityKeyId,
                participantEntitySetId,
                participantEntityKeyId,
                participantId,
                deviceId );
        createAppDataEntitiesAndAssociations( dataApi,
                data,
                organizationId,
                deviceEntityKeyId,
                participantId,
                participantEntityKeyId,
                participantEntitySetId );
        updateParticipantMetadata( dataApi,
                dataIntegrationApi,
                data,
                organizationId,
                participantEntitySetId,
                participantEntityKeyId,
                participantId );
        //  TODO:s Make sure to return any errors??? Currently void method.
        return data.size();
    }

    @Override
    public UUID registerDatasource(
            UUID organizationId,
            UUID studyId,
            String participantId,
            String datasourceId,
            com.google.common.base.Optional<Datasource> datasource ) {

        //  previous logic already verified the participant and that the device is not already connected.
        //  add the device and associate to the participant and to the study
        //  this will be two associations device --> person, device --> study
        //  aka write a line to these association tables, createAssociationData() does not exist in lattice-js yet.
        //  DataApi.createEntityAndAssociationData() see example above for template

        try {
            ApiClient apiClient = prodApiClientCache.get( ApiClient.class );
            DataApi dataApi = apiClient.getDataApi();
            DataIntegrationApi dataIntegrationApi = apiClient.getDataIntegrationApi();

            // entity set ids
            UUID devicesESID = ensureEntitySetExists( organizationId, CHRONICLE_DATA_COLLECTION, DEVICE );
            UUID usedByESID = ensureEntitySetExists( organizationId, CHRONICLE_DATA_COLLECTION, USED_BY );
            UUID studyESID = ensureEntitySetExists( organizationId, CHRONICLE, STUDIES );
            UUID participantsESID = ensureEntitySetExists( organizationId, CHRONICLE, PARTICIPANTS );

            // ensure study and participant exist
            UUID studyEKID = Preconditions
                    .checkNotNull( getStudyEntityKeyId( organizationId, studyId ), "study must exist" );
            UUID participantEKID = Preconditions
                    .checkNotNull( getParticipantEntityKeyId( organizationId, studyId, participantId ) );

            // device entity data
            Map<UUID, Set<Object>> deviceData = new HashMap<>();
            deviceData.put( propertyTypeIdsByFQN.get( STRING_ID_FQN ), Sets.newHashSet( datasourceId ) );

            if ( datasource.isPresent() && AndroidDevice.class.isAssignableFrom( datasource.get().getClass() ) ) {
                AndroidDevice device = (AndroidDevice) datasource.get();
                deviceData.put( propertyTypeIdsByFQN.get( MODEL_FQN ), Sets.newHashSet( device.getModel() ) );
                deviceData.put( propertyTypeIdsByFQN.get( VERSION_FQN ), Sets.newHashSet( device.getOsVersion() ) );
            }

            UUID deviceEntityKeyId = reserveDeviceEntityKeyId( devicesESID, deviceData, dataIntegrationApi );
            if ( deviceEntityKeyId == null ) {
                logger.error( "Unable to reserve deviceEntityKeyId, dataSourceId = {}, studyId = {}, participantId = {}",
                        datasourceId,
                        studyId,
                        participantId );
                return null;
            }
            dataApi.updateEntitiesInEntitySet( devicesESID,
                    ImmutableMap.of( deviceEntityKeyId, deviceData ),
                    UpdateType.Merge );

            EntityDataKey deviceEDK = new EntityDataKey( devicesESID, deviceEntityKeyId );
            EntityDataKey participantEDK = new EntityDataKey( participantsESID, participantEKID );
            EntityDataKey studyEDK = new EntityDataKey( studyESID, studyEKID );

            ListMultimap<UUID, DataEdge> associations = ArrayListMultimap.create();

            Map<UUID, Set<Object>> usedByEntity = ImmutableMap
                    .of( propertyTypeIdsByFQN.get( STRING_ID_FQN ), Sets.newHashSet( UUID.randomUUID() ) );

            associations.put( usedByESID, new DataEdge( deviceEDK, participantEDK, usedByEntity ) );
            associations.put( usedByESID, new DataEdge( deviceEDK, studyEDK, usedByEntity ) );

            dataApi.createAssociations( associations );

            return deviceEntityKeyId;
        } catch ( Exception e ) {

            String error = "unable to register device: "
                    + "organizationId = " + organizationId
                    + ", studyId = " + studyId
                    + ", deviceId = " + datasourceId;
            logger.error( error, e );
            throw new RuntimeException( error );
        }
    }

    @Override
    public boolean isNotificationsEnabled( UUID organizationId, UUID studyId ) {
        logger.info( "Checking notifications enabled on studyId = {}, organization = {}", studyId, organizationId );

        try {
            ApiClient apiClient = prodApiClientCache.get( ApiClient.class );
            SearchApi searchApi = apiClient.getSearchApi();

            // get entity set ids
            UUID studyESID = ensureEntitySetExists( organizationId, CHRONICLE, STUDIES );
            UUID notificationESID = ensureEntitySetExists( organizationId, CHRONICLE, NOTIFICATION );
            UUID partOfESID = ensureEntitySetExists( organizationId, CHRONICLE, PART_OF );

            // ensure study exists
            UUID studyEKID = Preconditions
                    .checkNotNull( getStudyEntityKeyId( organizationId, studyId ), "study entity must exist" );

            Map<UUID, List<NeighborEntityDetails>> neighbors = searchApi
                    .executeFilteredEntityNeighborSearch(
                            studyESID,
                            new EntityNeighborsFilter(
                                    ImmutableSet.of( studyEKID ),
                                    Optional.of( ImmutableSet.of( notificationESID ) ),
                                    Optional.of( ImmutableSet.of( studyESID ) ),
                                    Optional.of( ImmutableSet.of( partOfESID ) )
                            )
                    );

            /*
             * studies with notifications enabled have 'ol.id' property in the corresponding
             * associationDetails set to the value of the studyId
             * For each study, there is can only be at most 1 notification -> partof -> study association,
             * therefore it suffices to explore the first neighbor
             */
            return neighbors
                    .values()
                    .stream()
                    .flatMap( Collection::stream )
                    .anyMatch( neighbor -> neighbor.getAssociationDetails().getOrDefault( OL_ID_FQN, Set.of( "" ) )
                            .iterator().next().toString().equals( studyId.toString() ) );

        } catch ( Exception e ) {
            String error =
                    "failed to get notification enabled status for study " + studyId + " in org " + organizationId;
            logger.error( error, e );
            throw new RuntimeException( error );
        }
    }

    @Override
    public UUID getDeviceEntityKeyId( UUID organizationId, UUID studyId, String participantId, String datasourceId ) {
        logger.info(
                "Getting device entity key id, organizationId = {}, studyId = {}, participantId = {}, datasourceId = {}",
                organizationId,
                studyId,
                participantId,
                datasourceId );
        return deviceEntityKeyIdsByOrg.getOrDefault( organizationId, Map.of() ).getOrDefault( datasourceId, null );
    }

    @Override
    public boolean isKnownDatasource( UUID organizationId, UUID studyId, String participantId, String datasourceId ) {
        return getDeviceEntityKeyId( organizationId, studyId, participantId, datasourceId ) != null;
    }

    private void deleteStudyData(
            UUID organizationId,
            UUID studyId,
            Optional<String> participantId,
            com.openlattice.data.DeleteType deleteType,
            String userToken ) {
        try {
            // load api for actions authenticated by the user
            ApiClient userApiClient = new ApiClient( RetrofitFactory.Environment.PROD_INTEGRATION, () -> userToken );
            SearchApi userSearchApi = userApiClient.getSearchApi();
            DataApi userDataApi = userApiClient.getDataApi();

            // get required entity set ids
            UUID studiesESID = ensureEntitySetExists( organizationId, CHRONICLE, STUDIES );
            UUID participantsESID = ensureEntitySetExists( organizationId, CHRONICLE, PARTICIPANTS );

            // these entity set ids will be null if the respective app modules have not been installed for the organization
            UUID appDataESID = getEntitySetId( organizationId, CHRONICLE_DATA_COLLECTION, APPDATA );
            UUID preprocessedDataESID = getEntitySetId( organizationId, CHRONICLE_DATA_COLLECTION, PREPROCESSED_DATA );
            UUID devicesESID = getEntitySetId( organizationId, CHRONICLE_DATA_COLLECTION, DEVICE );
            UUID answersESID = getEntitySetId( organizationId, CHRONICLE_SURVEYS, ANSWER );

            // ensure study exists
            UUID studyEntityKeyId = Preconditions
                    .checkNotNull( getStudyEntityKeyId( organizationId, studyId ), "study must exist" );

            // get a set of all participants to remove:
            Set<UUID> participantsToRemove = new HashSet<>();
            if ( participantId.isPresent() ) {
                // if participantId: add to set
                UUID participantEntityKeyId = getParticipantEntityKeyId( organizationId, studyId, participantId.get() );
                if ( participantEntityKeyId == null ) {
                    throw new Exception(
                            "unable to delete participant " + participantId + ": participant does not exist." );
                }
                participantsToRemove.add( participantEntityKeyId );
            } else {
                // if no participant Id: load all participants and add to set
                userDataApi
                        .loadEntitySetData( participantsESID, FileType.json, userToken )
                        .forEach( entity -> entity.get( ID_FQN ).forEach( personId ->
                                        participantsToRemove.add( UUID.fromString( personId.toString() ) )
                                )
                        );
            }

            // Be super careful here that the mapping is one-to-one:
            // don't delete neighbors that might have other neighbors/participants

            Set<UUID> srcNeighborSetIds = new HashSet<>(), dstNeighborSetIds = new HashSet<>();

            if ( devicesESID != null )
                srcNeighborSetIds.add( devicesESID );
            if ( appDataESID != null )
                srcNeighborSetIds.add( appDataESID );
            if ( preprocessedDataESID != null )
                srcNeighborSetIds.add( preprocessedDataESID );

            if ( answersESID != null )
                dstNeighborSetIds.add( answersESID );

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
                                        participantsESID,
                                        new EntityNeighborsFilter(
                                                Set.of( participantEntityKeyId ),
                                                Optional.of( srcNeighborSetIds ),
                                                Optional.of( dstNeighborSetIds ),
                                                Optional.empty()
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
                            ( entitySetId, entityKeyId ) -> userDataApi
                                    .deleteEntities( entitySetId, entityKeyId, deleteType )
                    );

            // delete participants
            Integer deleted = userDataApi.deleteEntities( participantsESID, participantsToRemove, deleteType );
            logger.info( "Deleted {} entities for participant {}.", deleted, participantId );

            // delete study if no participantId is specified
            if ( participantId.isEmpty() ) {
                // delete participant entity set
                // userEntitySetsApi.deleteEntitySet( participantsESID ); // uncommenting this since entity set is shared by studies in the same org
                logger.info( "Deleted participant dataset for study {}.", studyId );
                userDataApi.deleteEntities( studiesESID,
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
            UUID organizationId,
            UUID studyId,
            String participantId,
            DeleteType deleteType,
            String userToken ) {
        com.openlattice.data.DeleteType deleteTypeTransformed = com.openlattice.data.DeleteType
                .valueOf( deleteType.toString() );
        deleteStudyData( organizationId, studyId, Optional.of( participantId ), deleteTypeTransformed, userToken );
        logger.info( "Successfully removed a participant from {}", studyId );
    }

    @Override
    public void deleteStudyAndAllNeighbors(
            UUID organizationId,
            UUID studyId,
            DeleteType deleteType,
            String userToken ) {
        com.openlattice.data.DeleteType deleteTypeTransformed = com.openlattice.data.DeleteType
                .valueOf( deleteType.toString() );
        deleteStudyData( organizationId, studyId, Optional.empty(), deleteTypeTransformed, userToken );
        logger.info( "Successfully removed study {}", studyId );
    }

    @Override
    public boolean isKnownParticipant( UUID organizationId, UUID studyId, String participantId ) {
        return getParticipantEntityKeyId( organizationId, studyId, participantId ) != null;
    }

    @Override
    public Map<String, UUID> getPropertyTypeIds( Set<String> propertyTypeFqns ) {
        EdmApi edmApi;
        try {
            ApiClient apiClient = prodApiClientCache.get( ApiClient.class );
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

    @Scheduled( fixedRate = STUDY_INFO_REFRESH_INTERVAL )
    public void refreshStudyInformation() {
        logger.info( "refreshing study information" );

        try {
            ApiClient apiClient = prodApiClientCache.get( ApiClient.class );
            DataApi dataApi = apiClient.getDataApi();
            SearchApi searchApi = apiClient.getSearchApi();
            PrincipalApi principalApi = apiClient.getPrincipalApi();

            // sync user
            principalApi.syncCallingUser();

            Map<UUID, Map<UUID, Map<String, UUID>>> allParticipants = Maps
                    .newHashMap(); // orgId -> studyId -> participantId -> participantEKID
            Map<UUID, Map<UUID, UUID>> studyEntityKeyIds = Maps.newHashMap(); // orgId -> studyId -> studyEKID
            Map<UUID, Map<String, UUID>> deviceEntityKeyIds = Maps.newHashMap(); // orgId -> deviceId -> deviceEKID

            Map<UUID, Map<CollectionTemplateTypeName, UUID>> orgEntitySets = entitySetIdsByOrgId
                    .getOrDefault( CHRONICLE, Map.of() );

            orgEntitySets.forEach( ( orgId, templateTypeESIDMap ) -> {

                Map<UUID, UUID> studyIds = Maps
                        .newHashMap(); // studyEKID -> studyId (for lookup when processing neighbors)

                // entity set ids
                UUID studiesESID = templateTypeESIDMap.getOrDefault( STUDIES, null );
                UUID participantsESID = templateTypeESIDMap.getOrDefault( PARTICIPANTS, null );
                UUID participatedInESID = templateTypeESIDMap.getOrDefault( PARTICIPATED_IN, null );
                UUID usedByESID = entitySetIdsByOrgId.getOrDefault( CHRONICLE_DATA_COLLECTION, Map.of() )
                        .getOrDefault( orgId, Map.of() ).getOrDefault( USED_BY, null );
                UUID deviceESID = entitySetIdsByOrgId.getOrDefault( CHRONICLE_DATA_COLLECTION, Map.of() )
                        .getOrDefault( orgId, Map.of() ).getOrDefault( DEVICE, null );

                if ( studiesESID == null || participantsESID == null )
                    return;

                Iterable<SetMultimap<FullQualifiedName, Object>> studyEntities = dataApi
                        .loadSelectedEntitySetData(
                                studiesESID,
                                new EntitySetSelection(
                                        Optional.of( ImmutableSet.of(
                                                propertyTypeIdsByFQN.get( STRING_ID_FQN )
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

                    Map<UUID, UUID> mappedEntities = studyEntityKeyIds.getOrDefault( orgId, Maps.newHashMap() );
                    mappedEntities.put( studyId, studyEKID );
                    studyEntityKeyIds.put( orgId, mappedEntities );
                    studyIds.put( studyEKID, studyId );
                } );

                // get study neighbors constrained by used_by and participated_in associations
                Set<UUID> edgeESIDS = Sets.newHashSet( participatedInESID, usedByESID )
                        .stream().filter( Objects::nonNull )
                        .collect( Collectors.toSet() );

                Set<UUID> srcESIDS = Sets.newHashSet( deviceESID, participantsESID )
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
                        if ( neighbor.getAssociationEntitySet().getId().equals( usedByESID ) ) {
                            String deviceId = getFirstValueOrNull( neighbor.getNeighborDetails().get(), STRING_ID_FQN );
                            if ( deviceId == null )
                                return;

                            UUID deviceEKID = neighbor.getNeighborId().get();

                            Map<String, UUID> deviceEntities = deviceEntityKeyIds
                                    .getOrDefault( orgId, Maps.newHashMap() );
                            deviceEntities.put( deviceId, deviceEKID );

                            deviceEntityKeyIds.put( orgId, deviceEntities );
                        } else {
                            // edge: participatedIn
                            String participantId = getFirstValueOrNull( neighbor.getNeighborDetails().get(),
                                    PERSON_ID_FQN );
                            if ( participantId == null )
                                return;

                            UUID participantEKID = neighbor.getNeighborId().get();

                            Map<String, UUID> participants = participantsByStudy
                                    .getOrDefault( studyId, Maps.newHashMap() );
                            participants.put( participantId, participantEKID );

                            participantsByStudy.put( studyId, participants );
                        }
                    } );
                } );

                allParticipants.put( orgId, participantsByStudy );

            } );

            this.studyParticipantsByOrg.putAll( allParticipants );
            this.deviceEntityKeyIdsByOrg.putAll( deviceEntityKeyIds );
            this.studyEntityKeyIdsByOrg.putAll( studyEntityKeyIds );

            logger.info( "loaded {} study EKIDS",
                    studyEntityKeyIdsByOrg.values().stream().mapToLong( map -> map.values().size() ).sum() );
            logger.info( "loaded {} device EKIDS",
                    deviceEntityKeyIdsByOrg.values().stream().mapToLong( map -> map.values().size() ).sum() );
            logger.info( "loaded {} participants",
                    studyParticipantsByOrg.values().stream().flatMap( map -> map.values().stream() )
                            .mapToLong( map -> map.values().size() ).sum() );

        } catch ( Exception e ) {
            logger.error( "caught exception while refreshing study information", e );
        }
    }

    @Scheduled( fixedRate = 60000 )
    public void refreshUserAppsDictionary() {

        DataApi dataApi;
        String jwtToken;

        try {
            ApiClient apiClient = prodApiClientCache.get( ApiClient.class );
            dataApi = apiClient.getDataApi();
            jwtToken = auth0Client.getIdToken( username, password );
        } catch ( ExecutionException | Auth0Exception e ) {
            logger.error( "Caught an exception", e );
            return;
        }

        logger.info( "Refreshing chronicle user apps dictionary" );

        Iterable<SetMultimap<FullQualifiedName, Object>> entitySetData = dataApi.loadEntitySetData(
                appsDictionaryESID,
                FileType.json,
                jwtToken
        );
        logger.info(
                "Fetched {} items from user apps dictionary entity set",
                Iterators.size( entitySetData.iterator() )
        );

        Set<String> systemAppPackageNames = new HashSet<>();

        entitySetData.forEach( entity -> {
            try {
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
            } catch ( Exception e ) {
                logger.error( "caught exception while processing entities from user apps dictionary", e );
            }
        } );

        this.systemAppPackageNames.clear();
        this.systemAppPackageNames.addAll( systemAppPackageNames );

        logger.info( "Loaded {} system apps from user apps dictionary", systemAppPackageNames.size() );
    }

    private Iterable<Map<String, Set<Object>>> getParticipantDataHelper(
            UUID organizationId,
            UUID participantEntityKeyId,
            CollectionTemplateTypeName edgeTemplateName,
            CollectionTemplateTypeName sourceTemplateName,
            String token ) {

        try {
            ApiClient apiClient = new ApiClient( RetrofitFactory.Environment.PROD_INTEGRATION, () -> token );
            EntitySetsApi entitySetsApi = apiClient.getEntitySetsApi();
            SearchApi searchApi = apiClient.getSearchApi();
            EdmApi edmApi = apiClient.getEdmApi();

            /*
             * 1. get the relevant EntitySets
             */

            UUID participantESID = ensureEntitySetExists( organizationId, CHRONICLE, PARTICIPANTS );
            UUID srcESID = ensureEntitySetExists( organizationId, CHRONICLE_DATA_COLLECTION, sourceTemplateName );
            UUID edgeESID = ensureEntitySetExists( organizationId, CHRONICLE_DATA_COLLECTION, edgeTemplateName );

            Map<UUID, EntitySet> entitySetsById = entitySetsApi.getEntitySetsById(
                    ImmutableSet.of( participantESID, srcESID, edgeESID )
            );

            EntitySet participantsES = entitySetsById.get( participantESID );
            EntitySet sourceES = entitySetsById.get( srcESID );
            EntitySet edgeES = entitySetsById.get( edgeESID );

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
                            Optional.of( ImmutableSet.of( sourceES.getId() ) ),
                            Optional.of( ImmutableSet.of( participantsES.getId() ) ),
                            Optional.of( ImmutableSet.of( edgeES.getId() ) )
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
            UUID organizationId,
            UUID studyId,
            UUID participatedInEntityKeyId,
            String token ) {
        return getParticipantDataHelper(
                organizationId,
                participatedInEntityKeyId,
                RECORDED_BY,
                PREPROCESSED_DATA,
                token
        );
    }

    @Override
    public Iterable<Map<String, Set<Object>>> getAllParticipantData(
            UUID organizationId,
            UUID studyId,
            UUID participantEntityKeyId,
            String token ) {
        return getParticipantDataHelper(
                organizationId,
                participantEntityKeyId,
                RECORDED_BY,
                APPDATA,
                token
        );
    }

    @Override
    public Iterable<Map<String, Set<Object>>> getAllParticipantAppsUsageData(
            UUID organizationId,
            UUID studyId,
            UUID participantEntityKeyId,
            String token ) {
        return getParticipantDataHelper(
                organizationId,
                participantEntityKeyId,
                USED_BY,
                USER_APPS,
                token
        );
    }

    @Override
    @Nonnull
    public Map<FullQualifiedName, Set<Object>> getParticipantEntity(
            UUID organizationId,
            UUID studyId,
            UUID participantEntityKeyId ) {

        try {
            ApiClient apiClient = prodApiClientCache.get( ApiClient.class );
            DataApi dataApi = apiClient.getDataApi();

            UUID entitySetId = getEntitySetId( organizationId, CHRONICLE, PARTICIPANTS );
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
    public ParticipationStatus getParticipationStatus( UUID organizationId, UUID studyId, String participantId ) {
        logger.info( "getting participation status: orgId = {}, studyId = {}, participantId = {}",
                organizationId,
                studyId,
                participantId );

        try {
            ApiClient apiClient = prodApiClientCache.get( ApiClient.class );
            SearchApi searchApi = apiClient.getSearchApi();

            // entity set ids
            UUID studiesESID = ensureEntitySetExists( organizationId, CHRONICLE, STUDIES );
            UUID participatedInESID = ensureEntitySetExists( organizationId, CHRONICLE, PARTICIPATED_IN );
            UUID participantsESID = ensureEntitySetExists( organizationId, CHRONICLE, PARTICIPANTS );

            // participant must exist
            UUID participantEKID = Preconditions
                    .checkNotNull( getParticipantEntityKeyId( organizationId, studyId, participantId ),
                            "participant not found" );

            // filtered search on participants to get associated study entities
            Map<UUID, List<NeighborEntityDetails>> neighborResults = searchApi.executeFilteredEntityNeighborSearch(
                    participantsESID,
                    new EntityNeighborsFilter(
                            ImmutableSet.of( participantEKID ),
                            Optional.of( ImmutableSet.of() ),
                            Optional.of( ImmutableSet.of( studiesESID ) ),
                            Optional.of( ImmutableSet.of( participatedInESID ) )
                    )
            );

            return neighborResults.getOrDefault( participantEKID, List.of() )
                    .stream()
                    .filter( neighbor -> studyId.toString()
                            .equals( getFirstValueOrNull( neighbor.getNeighborDetails().orElse( Map.of() ),
                                    STRING_ID_FQN ) ) )
                    .map( neighbor -> neighbor.getAssociationDetails()
                            .getOrDefault( STATUS_FQN, Set.of( ParticipationStatus.UNKNOWN.toString() ) ).iterator()
                            .next().toString() )
                    .map( ParticipationStatus::valueOf )
                    .findFirst().orElse( ParticipationStatus.UNKNOWN );

        } catch ( Exception e ) {
            logger.error( "unable to get participation status for participant: {}, study: {}, organization: {}.",
                    participantId,
                    studyId,
                    organizationId );
            return ParticipationStatus.UNKNOWN;
        }
    }

    @Override
    public ChronicleQuestionnaire getQuestionnaire( UUID organizationId, UUID studyId, UUID questionnaireEKID ) {
        try {
            logger.info( "Retrieving questionnaire: orgId = {} studyId = {}, questionnaire EKID = {}",
                    organizationId,
                    studyId,
                    questionnaireEKID );

            UUID studyEKID = Preconditions
                    .checkNotNull( getStudyEntityKeyId( organizationId, studyId ), "invalid study: " + studyId );

            // get apis
            ApiClient apiClient = prodApiClientCache.get( ApiClient.class );
            SearchApi searchApi = apiClient.getSearchApi();

            // entity set ids
            UUID questionnaireESID = ensureEntitySetExists( organizationId, CHRONICLE_SURVEYS, SURVEY );
            UUID studiesESID = ensureEntitySetExists( organizationId, CHRONICLE, STUDIES );
            UUID partOfESID = ensureEntitySetExists( organizationId, CHRONICLE, PART_OF );
            UUID questionESID = ensureEntitySetExists( organizationId, CHRONICLE_SURVEYS, QUESTION );

            // Get questionnaires that neighboring study
            Map<UUID, List<NeighborEntityDetails>> neighbors = searchApi.executeFilteredEntityNeighborSearch(
                    studiesESID,
                    new EntityNeighborsFilter(
                            Set.of( studyEKID ),
                            Optional.of( Set.of( questionnaireESID ) ),
                            Optional.of( Set.of( studiesESID ) ),
                            Optional.of( Set.of( partOfESID ) )
                    )
            );

            // find questionnaire entity matching given entity key id
            if ( neighbors.containsKey( studyEKID ) ) {
                ChronicleQuestionnaire questionnaire = new ChronicleQuestionnaire();

                neighbors.get( studyEKID )
                        .stream()
                        .filter( neighbor -> questionnaireEKID
                                .equals( getFirstUUIDOrNull( neighbor.getNeighborDetails().orElse( Map.of() ),
                                        ID_FQN ) ) )
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
                                Optional.of( Set.of( questionESID ) ),
                                Optional.of( Set.of( questionnaireESID ) ),
                                Optional.of( Set.of( partOfESID ) )
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

    public Map<UUID, Map<FullQualifiedName, Set<Object>>> getStudyQuestionnaires( UUID organizationId, UUID studyId ) {
        try {
            logger.info( "Retrieving questionnaires for study :{}", studyId );

            // check if study is valid
            UUID studyEntityKeyId = Preconditions
                    .checkNotNull( getStudyEntityKeyId( organizationId, studyId ), "invalid studyId: " + studyId );

            // load apis
            ApiClient apiClient = prodApiClientCache.get( ApiClient.class );
            SearchApi searchApi = apiClient.getSearchApi();

            // entity set ids
            UUID questionnaireESID = ensureEntitySetExists( organizationId, CHRONICLE_SURVEYS, SURVEY );
            UUID studiesESID = ensureEntitySetExists( organizationId, CHRONICLE, STUDIES );
            UUID partOfESID = ensureEntitySetExists( organizationId, CHRONICLE, PART_OF );

            // filtered search on questionnaires ES to get neighbors of study
            Map<UUID, List<NeighborEntityDetails>> neighbors = searchApi
                    .executeFilteredEntityNeighborSearch(
                            studiesESID,
                            new EntityNeighborsFilter(
                                    Set.of( studyEntityKeyId ),
                                    Optional.of( Set.of( questionnaireESID ) ),
                                    Optional.of( Set.of( studiesESID ) ),
                                    Optional.of( Set.of( partOfESID ) )
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
            UUID organizationId,
            UUID studyId,
            String participantId,
            Map<UUID, Map<FullQualifiedName, Set<Object>>> questionnaireResponses ) {
        DataApi dataApi;
        try {
            logger.info( "submitting questionnaire: studyId = {}, participantId = {}", studyId, participantId );

            ApiClient apiClient = prodApiClientCache.get( ApiClient.class );
            dataApi = apiClient.getDataApi();

            // get entity set ids
            UUID participantESID = ensureEntitySetExists( organizationId, CHRONICLE, PARTICIPANTS );
            UUID answersESID = ensureEntitySetExists( organizationId, CHRONICLE_DATA_COLLECTION, ANSWER );
            UUID respondsWithESID = ensureEntitySetExists( organizationId, CHRONICLE_SURVEYS, RESPONDS_WITH );
            UUID addressesESID = ensureEntitySetExists( organizationId, CHRONICLE_SURVEYS, ADDRESSES );
            UUID questionsESID = ensureEntitySetExists( organizationId, CHRONICLE_SURVEYS, QUESTION );

            // participant must be valid
            UUID participantEKID = Preconditions
                    .checkNotNull( getParticipantEntityKeyId( organizationId, studyId, participantId ),
                            "participant not found" );

            ListMultimap<UUID, Map<UUID, Set<Object>>> entities = ArrayListMultimap.create();
            ListMultimap<UUID, DataAssociation> associations = ArrayListMultimap.create();

            OffsetDateTime dateTime = OffsetDateTime.now();

            List<UUID> questionEntityKeyIds = new ArrayList<>( questionnaireResponses.keySet() );
            for ( int i = 0; i < questionEntityKeyIds.size(); i++ ) {
                UUID questionEntityKeyId = questionEntityKeyIds.get( i );

                Map<UUID, Set<Object>> answerEntity = ImmutableMap.of(
                        propertyTypeIdsByFQN.get( VALUES_FQN ),
                        questionnaireResponses.get( questionEntityKeyId ).get( VALUES_FQN ) );
                entities.put( answersESID, answerEntity );

                // 1. create participant -> respondsWith -> answer association
                Map<UUID, Set<Object>> respondsWithEntity = ImmutableMap.of(
                        propertyTypeIdsByFQN.get( DATE_TIME_FQN ),
                        ImmutableSet.of( dateTime )
                );
                associations.put( respondsWithESID, new DataAssociation(
                        participantESID,
                        Optional.empty(),
                        Optional.of( participantEKID ),
                        answersESID,
                        Optional.of( i ),
                        Optional.empty(),
                        respondsWithEntity
                ) );

                // 2. create answer -> addresses -> question association
                Map<UUID, Set<Object>> addressesEntity = ImmutableMap.of(
                        propertyTypeIdsByFQN.get( COMPLETED_DATE_TIME_FQN ),
                        ImmutableSet.of( dateTime )
                );
                associations.put( addressesESID, new DataAssociation(
                        answersESID,
                        Optional.of( i ),
                        Optional.empty(),
                        questionsESID,
                        Optional.empty(),
                        Optional.of( questionEntityKeyId ),
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
