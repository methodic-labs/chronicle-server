package com.openlattice.chronicle.services.enrollment;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.openlattice.ApiUtil;
import com.openlattice.chronicle.data.ParticipationStatus;
import com.openlattice.chronicle.services.ApiCacheManager;
import com.openlattice.chronicle.services.CommonTasksManager;
import com.openlattice.chronicle.services.ScheduledTasksManager;
import com.openlattice.chronicle.sources.AndroidDevice;
import com.openlattice.chronicle.sources.Datasource;
import com.openlattice.client.ApiClient;
import com.openlattice.data.*;
import com.openlattice.data.requests.NeighborEntityDetails;
import com.openlattice.edm.EdmApi;
import com.openlattice.search.SearchApi;
import com.openlattice.search.requests.EntityNeighborsFilter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.AccessDeniedException;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.openlattice.chronicle.constants.AppComponent.CHRONICLE;
import static com.openlattice.chronicle.constants.AppComponent.CHRONICLE_DATA_COLLECTION;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.DEVICE;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.NOTIFICATION;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.PARTICIPATED_IN;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.PART_OF;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.STUDIES;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.USED_BY;
import static com.openlattice.chronicle.constants.EdmConstants.DEVICES_ES;
import static com.openlattice.chronicle.constants.EdmConstants.MODEL_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.NOTIFICATION_ES;
import static com.openlattice.chronicle.constants.EdmConstants.OL_ID_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.PARTICIPATED_IN_ES;
import static com.openlattice.chronicle.constants.EdmConstants.PART_OF_ES;
import static com.openlattice.chronicle.constants.EdmConstants.STATUS_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.STRING_ID_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.STUDY_ES;
import static com.openlattice.chronicle.constants.EdmConstants.USED_BY_ES;
import static com.openlattice.chronicle.constants.EdmConstants.VERSION_FQN;
import static com.openlattice.chronicle.util.ChronicleServerUtil.checkNotNullUUIDs;
import static com.openlattice.chronicle.util.ChronicleServerUtil.getFirstValueOrNull;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public class EnrollmentManagerService implements EnrollmentManager {
    protected static final Logger logger = LoggerFactory.getLogger( EnrollmentManagerService.class );

    private final ApiCacheManager       apiCacheManager;
    private final CommonTasksManager    commonTasksManager;
    private final ScheduledTasksManager scheduledTasksManager;

    public EnrollmentManagerService(
            ApiCacheManager apiCacheManager,
            CommonTasksManager commonTasksManager,
            ScheduledTasksManager scheduledTasksManager ) {
        this.apiCacheManager = apiCacheManager;
        this.commonTasksManager = commonTasksManager;
        this.scheduledTasksManager = scheduledTasksManager;
    }

    private UUID reserveDeviceEntityKeyId(
            UUID deviceEntitySetId,
            Map<UUID, Set<Object>> data,
            DataIntegrationApi dataIntegrationApi ) {

        EntityKey entityKey = new EntityKey(
                deviceEntitySetId,
                ApiUtil.generateDefaultEntityId(
                        ImmutableList.of( commonTasksManager.getPropertyTypeId( STRING_ID_FQN ) ),
                        data
                )
        );

        return dataIntegrationApi.getEntityKeyIds( ImmutableSet.of( entityKey ) ).iterator().next();
    }

    private UUID registerDatasourceHelper(
            UUID organizationId,
            UUID studyId,
            String participantId,
            String datasourceId,
            Optional<Datasource> datasource ) {

        try {
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            DataApi dataApi = apiClient.getDataApi();
            DataIntegrationApi dataIntegrationApi = apiClient.getDataIntegrationApi();

            // entity set ids
            UUID usedByESID = commonTasksManager
                    .getEntitySetId( organizationId, CHRONICLE_DATA_COLLECTION, USED_BY, USED_BY_ES );
            UUID studyESID = commonTasksManager.getEntitySetId( organizationId, CHRONICLE, STUDIES, STUDY_ES );
            UUID devicesESID = commonTasksManager
                    .getEntitySetId( organizationId, CHRONICLE_DATA_COLLECTION, DEVICE, DEVICES_ES );
            UUID participantsESID = commonTasksManager.getParticipantEntitySetId( organizationId, studyId );

            checkNotNullUUIDs( Sets.newHashSet( usedByESID, studyESID, participantsESID, devicesESID ) );

            // ensure study and participant exist
            UUID studyEKID = Preconditions
                    .checkNotNull( commonTasksManager.getStudyEntityKeyId( organizationId, studyId ),
                            "study must exist" );
            UUID participantEKID = Preconditions
                    .checkNotNull( commonTasksManager
                            .getParticipantEntityKeyId( organizationId, studyId, participantId ) );

            // device entity data
            Map<UUID, Set<Object>> deviceData = new HashMap<>();
            deviceData.put( commonTasksManager.getPropertyTypeId( STRING_ID_FQN ), Sets.newHashSet( datasourceId ) );

            if ( datasource.isPresent() && AndroidDevice.class.isAssignableFrom( datasource.get().getClass() ) ) {
                AndroidDevice device = (AndroidDevice) datasource.get();
                deviceData
                        .put( commonTasksManager.getPropertyTypeId( MODEL_FQN ), Sets.newHashSet( device.getModel() ) );
                deviceData.put( commonTasksManager.getPropertyTypeId( VERSION_FQN ),
                        Sets.newHashSet( device.getOsVersion() ) );
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
                    .of( commonTasksManager.getPropertyTypeId( STRING_ID_FQN ), Sets.newHashSet( UUID.randomUUID() ) );

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

    private UUID enrollSource(
            UUID organizationId,
            UUID studyId,
            String participantId,
            String datasourceId,
            Optional<Datasource> datasource ) {
        final boolean isKnownParticipant = isKnownParticipant( organizationId, studyId, participantId );
        final UUID deviceEKID = getDeviceEntityKeyId( organizationId, studyId, participantId, datasourceId );

        logger.info(
                "Attempting to enroll source... study {}, participant {}, and datasource {} ",
                studyId,
                participantId,
                datasourceId
        );
        logger.info( "isKnownParticipant {} = {}", participantId, isKnownParticipant );
        logger.info( "isKnownDatasource {} = {}", datasourceId, deviceEKID != null );

        if ( isKnownParticipant && deviceEKID == null ) {
            return registerDatasourceHelper( organizationId, studyId, participantId, datasourceId, datasource );
        } else if ( isKnownParticipant ) {
            return deviceEKID;
        } else {
            logger.error(
                    "Unable to enroll device for orgId {} study {}, participant {}, and datasource {} due valid participant = {} or valid device = {}",
                    organizationId,
                    studyId,
                    participantId,
                    datasourceId,
                    isKnownParticipant,
                    deviceEKID != null );
            throw new AccessDeniedException( "Unable to enroll device." );
        }
    }

    @Override
    public UUID registerDatasource(
            UUID organizationId,
            UUID studyId,
            String participantId,
            String datasourceId,
            Optional<Datasource> datasource ) {

        return enrollSource( organizationId, studyId, participantId, datasourceId, datasource );
    }

    @Override
    public UUID getDeviceEntityKeyId(
            UUID organizationId, UUID studyId, String participantId, String datasourceId ) {
        logger.info( "Getting device entity key id, studyId = {}, participantId = {}, datasourceId = {}",
                studyId,
                participantId,
                datasourceId );

        if ( organizationId != null ) {
            return scheduledTasksManager.deviceIdsByOrg.getOrDefault( organizationId, Map.of() )
                    .getOrDefault( datasourceId, null );
        }

        return scheduledTasksManager.deviceIdsByEKID.getOrDefault( datasourceId, null );
    }

    @Override
    public boolean isKnownDatasource(
            UUID organizationId, UUID studyId, String participantId, String datasourceId ) {

        return getDeviceEntityKeyId( organizationId, studyId, participantId, datasourceId ) != null;
    }

    @Override
    public boolean isKnownParticipant( UUID organizationId, UUID studyId, String participantId ) {
        return commonTasksManager.getParticipantEntityKeyId( organizationId, studyId, participantId ) != null;
    }

    @Override
    public Map<FullQualifiedName, Set<Object>> getParticipantEntity(
            UUID organizationId, UUID studyId, UUID participantEntityKeyId ) {
        try {
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            DataApi dataApi = apiClient.getDataApi();

            UUID entitySetId = commonTasksManager.getParticipantEntitySetId( organizationId, studyId );
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
    public ParticipationStatus getParticipationStatus(
            UUID organizationId, UUID studyId, String participantId ) {
        logger.info( "getting participation status: orgId = {}, studyId = {}, participantId = {}",
                organizationId,
                studyId,
                participantId );

        try {
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            SearchApi searchApi = apiClient.getSearchApi();

            // entity set ids
            UUID studiesESID = commonTasksManager.getEntitySetId( organizationId, CHRONICLE, STUDIES, STUDY_ES );
            UUID participatedInESID = commonTasksManager.getEntitySetId( organizationId, CHRONICLE, PARTICIPATED_IN,
                    PARTICIPATED_IN_ES );
            UUID participantsESID = commonTasksManager.getParticipantEntitySetId( organizationId, studyId );

            checkNotNullUUIDs( Sets.newHashSet( studiesESID, participatedInESID, participantsESID ) );

            // participant must exist
            UUID participantEKID = Preconditions
                    .checkNotNull( commonTasksManager
                                    .getParticipantEntityKeyId( organizationId, studyId, participantId ),
                            "participant not found" );

            // filtered search on participants to get associated study entities
            Map<UUID, List<NeighborEntityDetails>> neighborResults = searchApi.executeFilteredEntityNeighborSearch(
                    participantsESID,
                    new EntityNeighborsFilter(
                            ImmutableSet.of( participantEKID ),
                            java.util.Optional.of( ImmutableSet.of() ),
                            java.util.Optional.of( ImmutableSet.of( studiesESID ) ),
                            java.util.Optional.of( ImmutableSet.of( participatedInESID ) )
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
                    organizationId, e );
            return ParticipationStatus.UNKNOWN;
        }
    }

    @Override
    public boolean isNotificationsEnabled( UUID organizationId, UUID studyId ) {
        logger.info( "Checking notifications enabled on studyId = {}, organization = {}", studyId, organizationId );

        try {
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            SearchApi searchApi = apiClient.getSearchApi();

            // get entity set ids
            UUID studyESID = commonTasksManager.getEntitySetId( organizationId, CHRONICLE, STUDIES, STUDY_ES );
            UUID notificationESID = commonTasksManager
                    .getEntitySetId( organizationId, CHRONICLE, NOTIFICATION, NOTIFICATION_ES );
            UUID partOfESID = commonTasksManager.getEntitySetId( organizationId, CHRONICLE, PART_OF, PART_OF_ES );

            // ensure study exists
            UUID studyEKID = Preconditions
                    .checkNotNull( commonTasksManager.getStudyEntityKeyId( organizationId, studyId ),
                            "study entity must exist" );

            Map<UUID, List<NeighborEntityDetails>> neighbors = searchApi
                    .executeFilteredEntityNeighborSearch(
                            studyESID,
                            new EntityNeighborsFilter(
                                    ImmutableSet.of( studyEKID ),
                                    java.util.Optional.of( ImmutableSet.of( notificationESID ) ),
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
    public Map<String, UUID> getPropertyTypeIds( Set<String> propertyTypeFqns ) {
        EdmApi edmApi;
        try {
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            edmApi = apiClient.getEdmApi();
        } catch ( ExecutionException e ) {
            logger.error( "Unable to load EdmApi" );
            return ImmutableMap.of();
        }

        return propertyTypeFqns.stream().map( FullQualifiedName::new ).map( fqn -> Pair
                .of( fqn.getFullQualifiedNameAsString(),
                        edmApi.getPropertyTypeId( fqn.getNamespace(), fqn.getName() ) ) )
                .collect( Collectors.toMap( Pair::getLeft, Pair::getRight ) );
    }
}
