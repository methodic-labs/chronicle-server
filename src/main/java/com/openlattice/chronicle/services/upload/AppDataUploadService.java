package com.openlattice.chronicle.services.upload;

import com.dataloom.streams.StreamUtil;
import com.google.common.base.Stopwatch;
import com.google.common.collect.*;
import com.openlattice.ApiUtil;
import com.openlattice.chronicle.data.EntitiesAndEdges;
import com.openlattice.chronicle.data.ParticipationStatus;
import com.openlattice.chronicle.services.ApiCacheManager;
import com.openlattice.chronicle.services.ScheduledTasksManager;
import com.openlattice.chronicle.services.edm.EdmCacheManager;
import com.openlattice.chronicle.services.enrollment.EnrollmentManager;
import com.openlattice.client.ApiClient;
import com.openlattice.data.*;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.openlattice.chronicle.constants.AppComponent.CHRONICLE;
import static com.openlattice.chronicle.constants.AppComponent.CHRONICLE_DATA_COLLECTION;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.APPDATA;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.DEVICE;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.HAS;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.METADATA;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.RECORDED_BY;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.USED_BY;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.USER_APPS;
import static com.openlattice.chronicle.constants.EdmConstants.DATA_ES;
import static com.openlattice.chronicle.constants.EdmConstants.DATE_LOGGED_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.DATE_TIME_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.DEVICES_ES;
import static com.openlattice.chronicle.constants.EdmConstants.END_DATE_TIME_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.FULL_NAME_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.HAS_ES;
import static com.openlattice.chronicle.constants.EdmConstants.METADATA_ES;
import static com.openlattice.chronicle.constants.EdmConstants.OL_ID_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.PERSON_ID_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.RECORDED_BY_ES;
import static com.openlattice.chronicle.constants.EdmConstants.RECORDED_DATE_TIME_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.START_DATE_TIME_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.STRING_ID_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.TITLE_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.USED_BY_ES;
import static com.openlattice.chronicle.constants.EdmConstants.USER_APPS_ES;
import static com.openlattice.chronicle.constants.OutputConstants.MINIMUM_DATE;
import static com.openlattice.chronicle.util.ChronicleServerUtil.checkNotNullUUIDs;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public class AppDataUploadService implements AppDataUploadManager {
    protected final Logger logger = LoggerFactory.getLogger( AppDataUploadService.class );

    private final ScheduledTasksManager scheduledTasksManager;
    private final EnrollmentManager     enrollmentManager;
    private final ApiCacheManager       apiCacheManager;
    private final EdmCacheManager       edmCacheManager;

    public AppDataUploadService(
            ApiCacheManager apiCacheManager,
            EdmCacheManager edmCacheManager,
            ScheduledTasksManager scheduledTasksManager,
            EnrollmentManager enrollmentManager ) {

        this.edmCacheManager = edmCacheManager;
        this.scheduledTasksManager = scheduledTasksManager;
        this.enrollmentManager = enrollmentManager;
        this.apiCacheManager = apiCacheManager;
    }

    // return an OffsetDateTime with time 00:00
    private String getMidnightDateTime( String dateTime ) {
        if ( dateTime == null )
            return null;

        return OffsetDateTime
                .parse( dateTime )
                .withHour( 0 )
                .withMinute( 0 )
                .withSecond( 0 )
                .withNano( 0 )
                .toString();
    }

    // unique for user + app + date
    private EntityKey getUsedByEntityKey( UUID usedByESID, Map<UUID, Set<Object>> entityData ) {

        return new EntityKey(
                usedByESID,
                ApiUtil.generateDefaultEntityId(
                        ImmutableList.of(
                                edmCacheManager.getPropertyTypeId( FULL_NAME_FQN ),
                                edmCacheManager.getPropertyTypeId( DATE_TIME_FQN ),
                                edmCacheManager.getPropertyTypeId( PERSON_ID_FQN )
                        ),
                        entityData
                )
        );
    }

    // unique for app + device + date
    private EntityKey getRecordedByEntityKey( UUID recordedByESID, Map<UUID, Set<Object>> entityData ) {

        return new EntityKey(
                recordedByESID,
                ApiUtil.generateDefaultEntityId(
                        ImmutableList.of(
                                edmCacheManager.getPropertyTypeId( DATE_LOGGED_FQN ),
                                edmCacheManager.getPropertyTypeId( STRING_ID_FQN ),
                                edmCacheManager.getPropertyTypeId( FULL_NAME_FQN )
                        ),
                        entityData
                )

        );
    }

    private EntityKey getUserAppsEntityKey( UUID userAppsESID, Map<UUID, Set<Object>> entityData ) {

        return new EntityKey(
                userAppsESID,
                ApiUtil.generateDefaultEntityId( ImmutableList
                                .of( edmCacheManager.getPropertyTypeId( FULL_NAME_FQN ) ),
                        entityData )
        );
    }

    private EntityKey getMetadataEntityKey( UUID metadataESID, Map<UUID, Set<Object>> entityData ) {

        return new EntityKey(
                metadataESID,
                ApiUtil.generateDefaultEntityId( ImmutableList.of( edmCacheManager.getPropertyTypeId( OL_ID_FQN ) ),
                        entityData )
        );
    }

    private EntityKey getHasEntityKey( UUID hasESID, Map<UUID, Set<Object>> entityData ) {

        return new EntityKey(
                hasESID,
                ApiUtil.generateDefaultEntityId( ImmutableList.of( edmCacheManager.getPropertyTypeId( OL_ID_FQN ) ),
                        entityData )
        );
    }

    // HELPER METHODS: upload
    private Map<UUID, Set<Object>> getUsedByEntity( String appPackageName, String dateLogged, String participantId ) {
        Map<UUID, Set<Object>> entity = new HashMap<>();

        entity.put( edmCacheManager.getPropertyTypeId( DATE_TIME_FQN ), ImmutableSet.of( dateLogged ) );
        entity.put( edmCacheManager.getPropertyTypeId( FULL_NAME_FQN ), ImmutableSet.of( appPackageName ) );
        entity.put( edmCacheManager.getPropertyTypeId( PERSON_ID_FQN ), ImmutableSet.of( participantId ) );

        return entity;
    }

    private Map<UUID, Set<Object>> getRecordedByEntity( String deviceId, String appPackageName, String dateLogged ) {
        Map<UUID, Set<Object>> entity = new HashMap<>();

        entity.put( edmCacheManager.getPropertyTypeId( FULL_NAME_FQN ), ImmutableSet.of( appPackageName ) );
        entity.put( edmCacheManager.getPropertyTypeId( DATE_LOGGED_FQN ), ImmutableSet.of( dateLogged ) );
        entity.put( edmCacheManager.getPropertyTypeId( STRING_ID_FQN ), ImmutableSet.of( deviceId ) );

        return entity;
    }

    private Map<UUID, Set<Object>> getHasEntity( String firstDateTime ) {
        Map<UUID, Set<Object>> entity = Maps.newHashMap();
        entity.put( edmCacheManager.getPropertyTypeId( OL_ID_FQN ), Set.of( firstDateTime ) );

        return entity;
    }

    private Map<UUID, Set<Object>> getMetadataEntity( UUID participantEKID ) {
        Map<UUID, Set<Object>> entity = Maps.newHashMap();
        entity.put( edmCacheManager.getPropertyTypeId( OL_ID_FQN ), Set.of( participantEKID ) );

        return entity;
    }

    private Map<UUID, Set<Object>> getUserAppsEntity( String appPackageName, String appName ) {

        Map<UUID, Set<Object>> entity = new HashMap<>();

        entity.put( edmCacheManager.getPropertyTypeId( FULL_NAME_FQN ), ImmutableSet.of( appPackageName ) );
        entity.put( edmCacheManager.getPropertyTypeId( TITLE_FQN ), ImmutableSet.of( appName ) );

        return entity;
    }

    private EntityKey getDeviceEntityKey( UUID devicesESID, String deviceId ) {
        return new EntityKey(
                devicesESID,
                deviceId
        );
    }

    private EntityKey getParticipantEntityKey( UUID participantESID, String participantId ) {
        return new EntityKey(
                participantESID,
                participantId
        );
    }

    private Set<OffsetDateTime> getDateTimeValuesFromDeviceData( List<SetMultimap<UUID, Object>> data ) {
        Set<OffsetDateTime> dateTimes = new HashSet<>();
        data.forEach(
                entity -> {
                    // most date properties in the entity are of length 1
                    for ( Object date : entity.get( edmCacheManager.getPropertyTypeId( DATE_LOGGED_FQN ) ) ) {
                        OffsetDateTime parsedDateTime = OffsetDateTime
                                .parse( date.toString() );

                        // filter out problematic entities with dates in the sixties
                        if ( parsedDateTime.isAfter( MINIMUM_DATE ) ) {
                            dateTimes.add( parsedDateTime );
                        }
                    }
                }
        );
        return dateTimes;
    }

    private EntitiesAndEdges getMetadataEntitiesAndEdges(
            DataApi dataApi,
            DataIntegrationApi integrationApi,
            List<SetMultimap<UUID, Object>> data,
            UUID organizationId,
            UUID studyId,
            UUID participantESID,
            UUID participantEKID,
            String participantId
    ) {
        // entity set ids
        UUID metadataESID = edmCacheManager.getEntitySetId( organizationId, CHRONICLE, METADATA, METADATA_ES );
        UUID hasESID = edmCacheManager.getEntitySetId( organizationId, CHRONICLE, HAS, HAS_ES );

        checkNotNullUUIDs( ImmutableSet.of( metadataESID, hasESID ) );

        Map<EntityKey, Map<UUID, Set<Object>>> entitiesByEntityKey = Maps.newHashMap();
        Set<Triple<EntityKey, EntityKey, EntityKey>> edgesByEntityKey = Sets.newHashSet();

        // get all dates in new data batch
        Set<OffsetDateTime> pushedDateTimes = getDateTimeValuesFromDeviceData( data );
        if ( pushedDateTimes.size() == 0 ) {
            return null;
        }

        String firstDateTime = pushedDateTimes
                .stream()
                .min( OffsetDateTime::compareTo )
                // .orElse( null ) :commenting this out since pushedDateTimes can never have nulls
                .get()
                .toString();

        String lastDateTime = pushedDateTimes
                .stream()
                .max( OffsetDateTime::compareTo )
                // .orElse( null ) :commenting this out since pushedDateTimes can never have nulls
                .get()
                .toString();

        Set<Object> uniqueDates = pushedDateTimes
                .stream()
                .map( dt -> dt
                        .truncatedTo( ChronoUnit.DAYS )
                        .format( DateTimeFormatter.ISO_DATE_TIME ) )
                .collect( Collectors.toSet() );

        Map<UUID, Set<Object>> metadataEntityData = getMetadataEntity( participantEKID );
        EntityKey metadataEK = getMetadataEntityKey( metadataESID, metadataEntityData );

        // verify if there is already an entry of metadata for participant
        // error means there is no metadata yet.
        UUID metadataEntityKeyId = integrationApi.getEntityKeyIds( ImmutableSet.of( metadataEK ) ).iterator().next();
        Map<FullQualifiedName, Set<Object>> entity = new HashMap<>();
        try {
            entity = dataApi.getEntity( metadataESID, metadataEntityKeyId );
        } catch ( Exception exception ) {
            logger.error(
                    "failure while getting metadata entity {} - study = {} participant = {}",
                    metadataEntityKeyId,
                    studyId,
                    participantId,
                    exception
            );
        }
        metadataEntityData.put( edmCacheManager.getPropertyTypeId( START_DATE_TIME_FQN ),
                entity.getOrDefault( START_DATE_TIME_FQN, Set.of( firstDateTime ) ) );
        metadataEntityData.put( edmCacheManager.getPropertyTypeId( END_DATE_TIME_FQN ), Set.of( lastDateTime ) );
        uniqueDates.addAll( entity.getOrDefault( RECORDED_DATE_TIME_FQN, Set.of() ) );
        metadataEntityData.put( edmCacheManager.getPropertyTypeId( RECORDED_DATE_TIME_FQN ), uniqueDates );

        entitiesByEntityKey.put( metadataEK, metadataEntityData );

        Map<UUID, Set<Object>> hasEntityData = getHasEntity( firstDateTime );
        EntityKey hasEK = getHasEntityKey( hasESID, hasEntityData );
        entitiesByEntityKey.put( hasEK, hasEntityData );

        EntityKey participantEK = getParticipantEntityKey( participantESID, participantId );

        // association: participant  => has => metadata
        edgesByEntityKey.add( Triple.of( participantEK, hasEK, metadataEK ) );

        return new EntitiesAndEdges( entitiesByEntityKey, edgesByEntityKey );
    }

    private String getFirstValueOrNull( SetMultimap<UUID, Object> entity, FullQualifiedName fqn ) {
        UUID fqnId = edmCacheManager.getPropertyTypeId( fqn );
        Object value = Iterables.getFirst( entity.get( fqnId ), null );

        return value == null ? null : value.toString();
    }

    private Map<EntityKey, UUID> getEntityKeyIdMap(
            DataIntegrationApi integrationApi,
            Set<Triple<EntityKey, EntityKey, EntityKey>> edgesByEntityKey,
            Set<EntityKey> entityKeys,
            UUID participantESID,
            UUID devicesESID,
            UUID deviceEKID,
            UUID participantEKID,
            String participantId,
            String deviceId
    ) {
        Map<EntityKey, UUID> entityKeyIdMap = Maps.newHashMap();

        Set<EntityKey> orderedEntityKeys = Sets.newLinkedHashSet( entityKeys );
        edgesByEntityKey.forEach( triple -> {
            orderedEntityKeys.add( triple.getMiddle() );
            orderedEntityKeys.add( triple.getLeft() );
            orderedEntityKeys.add( triple.getRight() );
        } );

        List<UUID> entityKeyIds = integrationApi.getEntityKeyIds( orderedEntityKeys );

        List<EntityKey> entityKeyList = new ArrayList<>( orderedEntityKeys );
        for ( int i = 0; i < orderedEntityKeys.size(); ++i ) {
            entityKeyIdMap.put( entityKeyList.get( i ), entityKeyIds.get( i ) );
        }

        // others
        EntityKey participantEK = getParticipantEntityKey( participantESID, participantId );
        entityKeyIdMap.put( participantEK, participantEKID );

        EntityKey deviceEK = getDeviceEntityKey( devicesESID, deviceId );
        entityKeyIdMap.put( deviceEK, deviceEKID );

        return entityKeyIdMap;
    }

    // group entities by entity set id
    private Map<UUID, Map<UUID, Map<UUID, Set<Object>>>> groupEntitiesByEntitySetId(
            Map<EntityKey, Map<UUID, Set<Object>>> entitiesByEntityKey,
            Map<EntityKey, UUID> entityKeyIdMap
    ) {
        Map<UUID, Map<UUID, Map<UUID, Set<Object>>>> entityKeysByEntitySet = Maps.newHashMap();

        entitiesByEntityKey.forEach( ( entityKey, entity ) -> {
            UUID entitySetId = entityKey.getEntitySetId();
            UUID entityKeyId = entityKeyIdMap.get( entityKey );

            Map<UUID, Map<UUID, Set<Object>>> mappedEntity = entityKeysByEntitySet
                    .getOrDefault( entitySetId, Maps.newHashMap() );
            mappedEntity.put( entityKeyId, entity );

            entityKeysByEntitySet.put( entitySetId, mappedEntity );
        } );

        return entityKeysByEntitySet;
    }

    private Set<DataEdgeKey> getDataEdgeKeysFromEntityKeys(
            Set<Triple<EntityKey, EntityKey, EntityKey>> edgesByEntityKey,
            Map<EntityKey, UUID> entityKeyIdMap
    ) {
        return StreamUtil.stream( edgesByEntityKey )
                .map( triple -> {
                    UUID srcEKID = entityKeyIdMap.get( triple.getLeft() );
                    UUID edgeEKID = entityKeyIdMap.get( triple.getMiddle() );
                    UUID dstEKID = entityKeyIdMap.get( triple.getRight() );

                    UUID srcESID = triple.getLeft().getEntitySetId();
                    UUID edgeESID = triple.getMiddle().getEntitySetId();
                    UUID dstESID = triple.getRight().getEntitySetId();

                    EntityDataKey src = new EntityDataKey( srcESID, srcEKID );
                    EntityDataKey edge = new EntityDataKey( edgeESID, edgeEKID );
                    EntityDataKey dst = new EntityDataKey( dstESID, dstEKID );

                    return new DataEdgeKey( src, dst, edge );
                } )
                .collect( Collectors.toSet() );
    }

    private ListMultimap<UUID, DataAssociation> getAppDataAssociations(
            UUID deviceEntityKeyId,
            UUID participantEntityKeyId,
            UUID appDataESID,
            UUID devicesESID,
            UUID recordedByESID,
            UUID participantEntitySetId,
            OffsetDateTime timeStamp,
            int index
    ) {
        ListMultimap<UUID, DataAssociation> associations = ArrayListMultimap.create();

        Map<UUID, Set<Object>> recordedByEntity = ImmutableMap
                .of( edmCacheManager.getPropertyTypeId( DATE_LOGGED_FQN ), Sets.newHashSet( timeStamp ) );

        associations.put( recordedByESID, new DataAssociation(
                appDataESID,
                Optional.of( index ),
                Optional.empty(),
                devicesESID,
                Optional.empty(),
                Optional.of( deviceEntityKeyId ),
                recordedByEntity
        ) );

        associations.put( recordedByESID, new DataAssociation(
                appDataESID,
                Optional.of( index ),
                Optional.empty(),
                participantEntitySetId,
                Optional.empty(),
                Optional.of( participantEntityKeyId ),
                recordedByEntity
        ) );
        return associations;
    }

    private boolean hasUserAppPackageName( UUID organizationId, String packageName ) {
        if ( organizationId != null ) {
            return scheduledTasksManager.getUserAppsFullNamesByOrg().getOrDefault( packageName, ImmutableSet.of() )
                    .contains( organizationId );
        }
        return scheduledTasksManager.getUserAppsFullNameValues().contains( packageName );
    }

    // create entities and edges
    private void createEntitiesAndAssociations(
            DataApi dataApi,
            DataIntegrationApi dataIntegrationApi,
            List<SetMultimap<UUID, Object>> data,
            UUID organizationId,
            UUID studyId,
            UUID deviceEKID,
            String participantId,
            String deviceId,
            UUID participantEKID,
            UUID participantESID
    ) {
        // entity set ids
        UUID appDataESID = edmCacheManager
                .getEntitySetId( organizationId, CHRONICLE_DATA_COLLECTION, APPDATA, DATA_ES );
        UUID recordedByESID = edmCacheManager
                .getEntitySetId( organizationId, CHRONICLE_DATA_COLLECTION, RECORDED_BY, RECORDED_BY_ES );
        UUID devicesESID = edmCacheManager
                .getEntitySetId( organizationId, CHRONICLE_DATA_COLLECTION, DEVICE, DEVICES_ES );
        UUID usedByESID = edmCacheManager
                .getEntitySetId( organizationId, CHRONICLE_DATA_COLLECTION, USED_BY, USED_BY_ES );
        UUID userAppsESID = edmCacheManager
                .getEntitySetId( organizationId, CHRONICLE_DATA_COLLECTION, USER_APPS, USER_APPS_ES );

        ListMultimap<UUID, Map<UUID, Set<Object>>> appDataEntities = ArrayListMultimap.create();
        ListMultimap<UUID, DataAssociation> appDataAssociations = ArrayListMultimap.create();

        Map<EntityKey, Map<UUID, Set<Object>>> entitiesByEntityKey = Maps.newHashMap();
        Set<Triple<EntityKey, EntityKey, EntityKey>> edgesByEntityKey = Sets.newHashSet();

        OffsetDateTime timeStamp = OffsetDateTime.now();

        for ( int i = 0; i < data.size(); i++ ) {
            SetMultimap<UUID, Object> appEntity = data.get( i );
            appDataEntities.put( appDataESID, Multimaps.asMap( appEntity ) );

            appDataAssociations
                    .putAll( getAppDataAssociations( deviceEKID,
                            participantEKID,
                            appDataESID,
                            devicesESID,
                            recordedByESID,
                            participantESID,
                            timeStamp,
                            i ) );

            String appPackageName, appName;
            appPackageName = appName = getFirstValueOrNull( appEntity, FULL_NAME_FQN );
            String dateLogged = getMidnightDateTime( getFirstValueOrNull( appEntity, DATE_LOGGED_FQN ) );

            if ( scheduledTasksManager.getSystemAppPackageNames().contains( appPackageName ) || dateLogged == null ) {
                continue; // 'system' app
            }

            if ( appEntity.containsKey( edmCacheManager.getPropertyTypeId( TITLE_FQN ) ) ) {
                appName = getFirstValueOrNull( appEntity, TITLE_FQN );
            }

            // association 1: user apps => recorded by => device
            Map<UUID, Set<Object>> userAppEntityData = getUserAppsEntity( appPackageName, appName );
            EntityKey userAppEK = getUserAppsEntityKey( userAppsESID, userAppEntityData );
            if ( !hasUserAppPackageName( organizationId, appPackageName ) ) {
                entitiesByEntityKey.put( userAppEK, userAppEntityData );
            }

            Map<UUID, Set<Object>> recordedByEntityData = getRecordedByEntity( deviceId, appPackageName, dateLogged );
            EntityKey recordedByEK = getRecordedByEntityKey( recordedByESID, recordedByEntityData );
            recordedByEntityData
                    .remove( edmCacheManager
                            .getPropertyTypeId( FULL_NAME_FQN ) );   // FULL_NAME_FQN is used to generate EKID but shouldn't be stored
            entitiesByEntityKey.put( recordedByEK, recordedByEntityData );

            EntityKey deviceEK = getDeviceEntityKey( devicesESID, deviceId );
            edgesByEntityKey.add( Triple.of( userAppEK, recordedByEK, deviceEK ) );

            // association 2: user apps => used by => participant
            Map<UUID, Set<Object>> usedByEntityData = getUsedByEntity( appPackageName, dateLogged, participantId );
            EntityKey usedByEK = getUsedByEntityKey( usedByESID, usedByEntityData );
            usedByEntityData.remove( edmCacheManager
                    .getPropertyTypeId( FULL_NAME_FQN ) ); // FULL_NAME_FQN shouldn't be stored
            usedByEntityData.remove( edmCacheManager
                    .getPropertyTypeId( PERSON_ID_FQN ) ); // PERSON_ID_FQN shouldn't be stored
            entitiesByEntityKey.put( usedByEK, usedByEntityData );

            EntityKey participantEK = getParticipantEntityKey( participantESID, participantId );
            edgesByEntityKey.add( Triple.of( userAppEK, usedByEK, participantEK ) );
        }
        EntitiesAndEdges metadata = getMetadataEntitiesAndEdges(
                dataApi,
                dataIntegrationApi,
                data,
                organizationId,
                studyId,
                participantESID,
                participantEKID,
                participantId
        );
        if ( metadata != null ) {
            entitiesByEntityKey.putAll( metadata.getEntityByEntityKey() );
            edgesByEntityKey.addAll( metadata.getEdgeEntityKeys() );
        }

        DataGraph dataGraph = new DataGraph( appDataEntities, appDataAssociations );
        dataApi.createEntityAndAssociationData( dataGraph );

        Map<EntityKey, UUID> entityKeyIdMap = getEntityKeyIdMap( dataIntegrationApi,
                edgesByEntityKey,
                entitiesByEntityKey.keySet(),
                participantESID,
                devicesESID,
                deviceEKID,
                participantEKID,
                participantId,
                deviceId
        );

        Map<UUID, Map<UUID, Map<UUID, Set<Object>>>> entitiesByESID = groupEntitiesByEntitySetId( entitiesByEntityKey,
                entityKeyIdMap );
        entitiesByESID.forEach( ( entitySetId, entities ) -> {
            dataApi.updateEntitiesInEntitySet( entitySetId, entities, UpdateType.PartialReplace );
        } );

        Set<DataEdgeKey> dataEdgeKeys = getDataEdgeKeysFromEntityKeys( edgesByEntityKey, entityKeyIdMap );
        dataApi.createEdges( dataEdgeKeys );
    }

    @Override
    public Integer upload(
            UUID organizationId,
            UUID studyId,
            String participantId,
            String deviceId,
            List<SetMultimap<UUID, Object>> data ) {
        Stopwatch stopwatch = Stopwatch.createStarted();

        logger.info( "attempting to log data: studyId = {}, participantId = {}, deviceId = {}",
                studyId,
                participantId,
                deviceId );
        try {
            ApiClient apiClient = apiCacheManager.intApiClientCache.get( ApiClient.class );
            DataApi dataApi = apiClient.getDataApi();
            DataIntegrationApi dataIntegrationApi = apiClient.getDataIntegrationApi();

            UUID participantEntityKeyId = enrollmentManager
                    .getParticipantEntityKeyId( organizationId, studyId, participantId );
            if ( participantEntityKeyId == null ) {
                logger.error( "Unable to retrieve participantEntityKeyId, studyId = {}, participantId = {}",
                        studyId,
                        participantId );
                return 0;
            }

            ParticipationStatus status = enrollmentManager
                    .getParticipationStatus( organizationId, studyId, participantId );
            if ( ParticipationStatus.NOT_ENROLLED.equals( status ) ) {
                logger.warn( "participantId = {} is not enrolled, ignoring data upload", participantId );
                return 0;
            }

            UUID deviceEntityKeyId = enrollmentManager
                    .getDeviceEntityKeyId( organizationId, studyId, participantId, deviceId );
            if ( deviceEntityKeyId == null ) {
                logger.error(
                        "deviceId not found, ignoring data upload: studyId = {}, participantId = {}, deviceId = {} ",
                        studyId,
                        participantId,
                        deviceId );
                return 0;
            }

            UUID participantEntitySetId = edmCacheManager.getParticipantEntitySetId( organizationId, studyId );
            checkNotNullUUIDs( ImmutableSet.of( participantEntitySetId ) );

            createEntitiesAndAssociations(
                    dataApi,
                    dataIntegrationApi,
                    data,
                    organizationId,
                    studyId,
                    deviceEntityKeyId,
                    participantId,
                    deviceId,
                    participantEntityKeyId,
                    participantEntitySetId
            );

        } catch ( Exception exception ) {
            logger.error( "error logging data: studyId = {}, participantId = {}, deviceId = {}",
                    studyId,
                    participantId,
                    deviceId,
                    exception
            );
            return 0;
        }

        stopwatch.stop();
        logger.info( "logging {} entries took {} seconds: studyId = {}, participantId = {}, deviceId = {}",
                data.size(),
                stopwatch.elapsed( TimeUnit.SECONDS ),
                studyId,
                participantId,
                deviceId
        );

        return data.size();
    }
}