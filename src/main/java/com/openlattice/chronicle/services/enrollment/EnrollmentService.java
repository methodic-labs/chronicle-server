package com.openlattice.chronicle.services.enrollment;

import com.google.common.base.Optional;
import com.google.common.collect.*;
import com.openlattice.ApiUtil;
import com.openlattice.chronicle.data.ChronicleCoreAppConfig;
import com.openlattice.chronicle.data.ChronicleDataCollectionAppConfig;
import com.openlattice.chronicle.data.ParticipationStatus;
import com.openlattice.chronicle.services.ApiCacheManager;
import com.openlattice.chronicle.services.ScheduledTasksManager;
import com.openlattice.chronicle.services.edm.EdmCacheManager;
import com.openlattice.chronicle.services.entitysets.EntitySetIdsManager;
import com.openlattice.chronicle.sources.AndroidDevice;
import com.openlattice.chronicle.sources.Datasource;
import com.openlattice.client.ApiClient;
import com.openlattice.data.*;
import com.openlattice.data.requests.NeighborEntityDetails;
import com.openlattice.search.SearchApi;
import com.openlattice.search.requests.EntityNeighborsFilter;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.AccessDeniedException;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.openlattice.chronicle.constants.EdmConstants.MODEL_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.OL_ID_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.STATUS_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.STRING_ID_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.VERSION_FQN;
import static com.openlattice.chronicle.util.ChronicleServerUtil.*;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public class EnrollmentService implements EnrollmentManager {
    protected static final Logger logger = LoggerFactory.getLogger( EnrollmentService.class );

    private final ApiCacheManager       apiCacheManager;
    private final ScheduledTasksManager scheduledTasksManager;
    private final EdmCacheManager       edmCacheManager;
    private final EntitySetIdsManager   entitySetIdsManager;

    public EnrollmentService(
            ApiCacheManager apiCacheManager,
            EdmCacheManager edmCacheManager,
            EntitySetIdsManager entitySetIdsManager,
            ScheduledTasksManager scheduledTasksManager ) {

        this.edmCacheManager = edmCacheManager;
        this.scheduledTasksManager = scheduledTasksManager;
        this.apiCacheManager = apiCacheManager;
        this.entitySetIdsManager = entitySetIdsManager;
    }

    private UUID reserveDeviceEntityKeyId(
            UUID deviceEntitySetId,
            Map<UUID, Set<Object>> data,
            DataIntegrationApi dataIntegrationApi ) {

        EntityKey entityKey = new EntityKey(
                deviceEntitySetId,
                ApiUtil.generateDefaultEntityId(
                        ImmutableList.of( edmCacheManager.getPropertyTypeId( STRING_ID_FQN ) ),
                        data
                )
        );

        return dataIntegrationApi.getEntityKeyIds( ImmutableSet.of( entityKey ) ).iterator().next();
    }

    private void updateDeviceIdsCache( UUID organizationId, UUID deviceEKID, String datasourceId ) {
        if ( organizationId != null ) {
            scheduledTasksManager
                    .getDeviceIdsByOrg().computeIfAbsent( organizationId, key -> Maps.newHashMap() )
                    .put( datasourceId, deviceEKID );
            return;
        }

        scheduledTasksManager.getDeviceIdsByEKID().put( datasourceId, deviceEKID );
    }

    private UUID registerDataSourceHelper(
            UUID organizationId,
            UUID studyId,
            String participantId,
            String dataSourceId,
            Optional<Datasource> dataSource ) {

        try {
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            DataApi dataApi = apiClient.getDataApi();
            DataIntegrationApi dataIntegrationApi = apiClient.getDataIntegrationApi();

            // entity set ids
            ChronicleDataCollectionAppConfig dataCollectionAppConfig = entitySetIdsManager
                    .getChronicleDataCollectionAppConfig( organizationId );
            ChronicleCoreAppConfig coreAppConfig = entitySetIdsManager
                    .getChronicleAppConfig( organizationId, getParticipantEntitySetName( studyId ) );

            UUID usedByESID = dataCollectionAppConfig.getUsedByEntitySetId();
            UUID studyESID = coreAppConfig.getStudiesEntitySetId();
            UUID devicesESID = dataCollectionAppConfig.getDeviceEntitySetId();
            UUID participantsESID = coreAppConfig.getParticipantEntitySetId();

            // ensure study and participant exist
            UUID studyEKID = checkNotNull( getStudyEntityKeyId( organizationId, studyId ), "study must exist" );
            UUID participantEKID = checkNotNull( getParticipantEntityKeyId( organizationId, studyId, participantId ) );

            // device entity data
            Map<UUID, Set<Object>> deviceData = new HashMap<>();
            deviceData.put( edmCacheManager.getPropertyTypeId( STRING_ID_FQN ), Sets.newHashSet( dataSourceId ) );

            if ( dataSource.isPresent() && AndroidDevice.class.isAssignableFrom( dataSource.get().getClass() ) ) {
                AndroidDevice device = (AndroidDevice) dataSource.get();
                deviceData
                        .put( edmCacheManager.getPropertyTypeId( MODEL_FQN ), Sets.newHashSet( device.getModel() ) );
                deviceData.put( edmCacheManager.getPropertyTypeId( VERSION_FQN ),
                        Sets.newHashSet( device.getOsVersion() ) );
            }

            UUID deviceEntityKeyId = reserveDeviceEntityKeyId( devicesESID, deviceData, dataIntegrationApi );
            if ( deviceEntityKeyId == null ) {
                logger.error( getLoggingMessage(
                        "unable to reserve ekid for data source",
                        organizationId,
                        studyId,
                        participantId,
                        dataSourceId
                ) );
                return null;
            }
            dataApi.updateEntitiesInEntitySet( devicesESID,
                    ImmutableMap.of( deviceEntityKeyId, deviceData ),
                    UpdateType.Merge );

            updateDeviceIdsCache( organizationId, deviceEntityKeyId, dataSourceId );

            EntityDataKey deviceEDK = new EntityDataKey( devicesESID, deviceEntityKeyId );
            EntityDataKey participantEDK = new EntityDataKey( participantsESID, participantEKID );
            EntityDataKey studyEDK = new EntityDataKey( studyESID, studyEKID );

            ListMultimap<UUID, DataEdge> associations = ArrayListMultimap.create();

            Map<UUID, Set<Object>> usedByEntity = ImmutableMap
                    .of( edmCacheManager.getPropertyTypeId( STRING_ID_FQN ), Sets.newHashSet( UUID.randomUUID() ) );

            associations.put( usedByESID, new DataEdge( deviceEDK, participantEDK, usedByEntity ) );
            associations.put( usedByESID, new DataEdge( deviceEDK, studyEDK, usedByEntity ) );

            dataApi.createAssociations( associations );

            logger.info( getLoggingMessage(
                    "data source registered",
                    organizationId,
                    studyId,
                    participantId,
                    dataSourceId
            ) );
            return deviceEntityKeyId;
        } catch ( Exception exception ) {
            logger.error( getLoggingMessage(
                    "unable to register data source",
                    organizationId,
                    studyId,
                    participantId,
                    dataSourceId
            ), exception );
            throw new RuntimeException( "unable to register data source" );
        }
    }

    @Override
    public UUID registerDataSource(
            UUID organizationId,
            UUID studyId,
            String participantId,
            String dataSourceId,
            Optional<Datasource> datasource ) {

        logger.info( getLoggingMessage(
                "attempting to register data source",
                organizationId,
                studyId,
                participantId,
                dataSourceId
        ) );

        final boolean isKnownParticipant = isKnownParticipant( organizationId, studyId, participantId );
        if ( !isKnownParticipant ) {
            logger.error( getLoggingMessage(
                    "unknown participant, unable to register data source",
                    organizationId,
                    studyId,
                    participantId,
                    dataSourceId
            ) );
            throw new AccessDeniedException( "unknown participant, unable to register data source" );
        }

        final UUID deviceEKID = getDeviceEntityKeyId( organizationId, studyId, participantId, dataSourceId );
        if ( deviceEKID != null ) {
            logger.info( getLoggingMessage(
                    "data source is registered",
                    organizationId,
                    studyId,
                    participantId,
                    dataSourceId
            ) );
            return deviceEKID;
        }

        return registerDataSourceHelper( organizationId, studyId, participantId, dataSourceId, datasource );
    }

    @Override
    public UUID getDeviceEntityKeyId( UUID organizationId, UUID studyId, String participantId, String datasourceId ) {
        if ( organizationId != null ) {
            return scheduledTasksManager
                    .getDeviceIdsByOrg()
                    .getOrDefault( organizationId, Map.of() )
                    .getOrDefault( datasourceId, null );
        }
        return scheduledTasksManager.getDeviceIdsByEKID().getOrDefault( datasourceId, null );
    }

    @Override
    public boolean isKnownDatasource( UUID organizationId, UUID studyId, String participantId, String datasourceId ) {
        return getDeviceEntityKeyId( organizationId, studyId, participantId, datasourceId ) != null;
    }

    @Override
    public boolean isKnownParticipant( UUID organizationId, UUID studyId, String participantId ) {
        return getParticipantEntityKeyId( organizationId, studyId, participantId ) != null;
    }

    @Override
    public Map<FullQualifiedName, Set<Object>> getParticipantEntity(
            UUID organizationId, UUID studyId, UUID participantEntityKeyId ) {
        try {
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            DataApi dataApi = apiClient.getDataApi();

            ChronicleCoreAppConfig coreAppConfig = entitySetIdsManager
                    .getChronicleAppConfig( organizationId, getParticipantEntitySetName( studyId ) );

            UUID entitySetId = coreAppConfig.getParticipantEntitySetId();
            if ( entitySetId == null ) {
                logger.error( "Unable to ge participant ESID: orgId = {}, studyId = {}, participantEKID = {}",
                        organizationId,
                        studyId,
                        participantEntityKeyId );
                return ImmutableMap.of();
            }
            Map<FullQualifiedName, Set<Object>> entity = dataApi.getEntity( entitySetId, participantEntityKeyId );
            if ( entity == null ) {
                logger.error(
                        "Unable to get participant entity: orgId = {}, studyId = {} participantEKID = {}, participantESID = {}",
                        organizationId,
                        studyId,
                        participantEntityKeyId,
                        entitySetId );
                return ImmutableMap.of();
            }
            return entity;

        } catch ( ExecutionException e ) {
            logger.error( "Unable to get participant entity: orgId = {}, studyId = {}, participantEKID = {} ",
                    organizationId,
                    studyId,
                    participantEntityKeyId,
                    e );
            return ImmutableMap.of();
        }
    }

    @Override
    public ParticipationStatus getParticipationStatus( UUID organizationId, UUID studyId, String participantId ) {

        ParticipationStatus status;

        logger.info( getLoggingMessage( "getting participation status", organizationId, studyId, participantId ) );

        try {
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            SearchApi searchApi = apiClient.getSearchApi();

            // entity set ids
            ChronicleCoreAppConfig coreAppConfig = entitySetIdsManager
                    .getChronicleAppConfig( organizationId, getParticipantEntitySetName( studyId ) );

            UUID studiesESID = coreAppConfig.getStudiesEntitySetId();
            UUID participatedInESID = coreAppConfig.getParticipatedInEntitySetId();
            UUID participantsESID = coreAppConfig.getParticipantEntitySetId();

            // participant must exist
            UUID participantEKID = checkNotNull(
                    getParticipantEntityKeyId( organizationId, studyId, participantId ),
                    getLoggingMessage( "participant not found", organizationId, studyId, participantId )
            );

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

            status = neighborResults
                    .getOrDefault( participantEKID, List.of() )
                    .stream()
                    .filter( neighbor -> studyId.toString().equals(
                            getFirstValueOrNull( neighbor.getNeighborDetails().orElse( Map.of() ), STRING_ID_FQN )
                    ) )
                    .map( neighbor -> getFirstValueOrNull( neighbor.getAssociationDetails(), STATUS_FQN ) )
                    .filter( Objects::nonNull )
                    .map( ParticipationStatus::valueOf )
                    .findFirst()
                    .orElse( ParticipationStatus.UNKNOWN );

            logger.info( getLoggingMessage(
                    String.format( "participation status = %s", status ),
                    organizationId,
                    studyId,
                    participantId
            ) );

            return status;
        } catch ( Exception exception ) {
            logger.info( getLoggingMessage(
                    String.format( "unable to get participation status, returning %s", ParticipationStatus.UNKNOWN ),
                    organizationId,
                    studyId,
                    participantId
            ), exception );
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
            ChronicleCoreAppConfig coreAppConfig = entitySetIdsManager.getChronicleAppConfig( organizationId );

            UUID studyESID = coreAppConfig.getStudiesEntitySetId();
            UUID notificationESID = coreAppConfig.getNotificationEntitySetId();
            UUID partOfESID = coreAppConfig.getPartOfEntitySetId();

            // ensure study exists
            UUID studyEKID = checkNotNull( getStudyEntityKeyId( organizationId, studyId ),
                    "study does not exist: orgId=%s, studyId=%s", organizationId, studyId );

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
                    .map( NeighborEntityDetails::getAssociationDetails )
                    .anyMatch( neighbor -> studyId.toString().equals( getFirstValueOrNull( neighbor, OL_ID_FQN ) ) );

        } catch ( Exception e ) {
            String error =
                    "failed to get notification enabled status for study " + studyId + " in org " + organizationId;
            logger.error( error, e );
            throw new RuntimeException( error );
        }
    }

    @Override
    public UUID getParticipantEntityKeyId( UUID organizationId, UUID studyId, String participantId ) {
        if ( organizationId != null ) {
            Map<UUID, Map<String, UUID>> participants = scheduledTasksManager.getStudyParticipantsByOrg()
                    .getOrDefault( organizationId, Map.of() );

            return participants.getOrDefault( studyId, Map.of() ).getOrDefault( participantId, null );
        }

        return scheduledTasksManager.getStudyParticipants().getOrDefault( studyId, Map.of() )
                .getOrDefault( participantId, null );
    }

    @Override
    public UUID getStudyEntityKeyId( UUID organizationId, UUID studyId ) {
        if ( organizationId != null ) {
            return scheduledTasksManager.getStudyEntityKeyIdsByOrg().getOrDefault( organizationId, Map.of() )
                    .getOrDefault( studyId, null );
        }
        return scheduledTasksManager.getStudyEKIDById().getOrDefault( studyId, null );
    }
}
