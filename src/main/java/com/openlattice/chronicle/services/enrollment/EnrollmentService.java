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
import com.openlattice.chronicle.sources.IOSDevice;
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
import static com.openlattice.chronicle.constants.EdmConstants.NAME_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.OL_ID_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.STATUS_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.STRING_ID_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.VERSION_FQN;
import static com.openlattice.chronicle.util.ChronicleServerUtil.ORG_STUDY_PARTICIPANT;
import static com.openlattice.chronicle.util.ChronicleServerUtil.ORG_STUDY_PARTICIPANT_DATASOURCE;
import static com.openlattice.chronicle.util.ChronicleServerUtil.getFirstValueOrNull;
import static com.openlattice.chronicle.util.ChronicleServerUtil.getParticipantEntitySetName;

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

    private Map<UUID, Set<Object>> createDeviceData( String datasourceId, Optional<Datasource> datasource ) {
        Map<UUID, Set<Object>> data = Maps.newHashMap();
        data.put( edmCacheManager.getPropertyTypeId( STRING_ID_FQN ), ImmutableSet.of( datasourceId ) );

        if ( datasource.isPresent() ) {
            if ( AndroidDevice.class.isAssignableFrom( datasource.get().getClass() ) ) {
                AndroidDevice device = (AndroidDevice) datasource.get();
                data.put( edmCacheManager.getPropertyTypeId( MODEL_FQN ), ImmutableSet.of( device.getModel() ) );
                data.put( edmCacheManager.getPropertyTypeId( VERSION_FQN ), ImmutableSet.of( device.getOsVersion() ) );
            }

            if ( IOSDevice.class.isAssignableFrom( datasource.get().getClass() ) ) {
                IOSDevice device = (IOSDevice) datasource.get();
                data.put( edmCacheManager.getPropertyTypeId( MODEL_FQN ),
                        ImmutableSet.of( device.getLocalizedModel() ) );
                data.put( edmCacheManager.getPropertyTypeId( VERSION_FQN ), ImmutableSet.of( device.getVersion() ) );
                data.put( edmCacheManager.getPropertyTypeId( NAME_FQN ), ImmutableSet.of( device.getName() ) );
            }
        }

        return data;
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

            Map<UUID, Set<Object>> deviceData = createDeviceData( datasourceId, datasource );

            UUID deviceEntityKeyId = reserveDeviceEntityKeyId( devicesESID, deviceData, dataIntegrationApi );
            if ( deviceEntityKeyId == null ) {
                logger.error(
                        "unable to reserve ekid for datasource" + ORG_STUDY_PARTICIPANT_DATASOURCE,
                        organizationId,
                        studyId,
                        participantId,
                        datasourceId
                );
                return null;
            }
            dataApi.updateEntitiesInEntitySet( devicesESID,
                    ImmutableMap.of( deviceEntityKeyId, deviceData ),
                    UpdateType.Merge,
                    PropertyUpdateType.Versioned );

            updateDeviceIdsCache( organizationId, deviceEntityKeyId, datasourceId );

            EntityDataKey deviceEDK = new EntityDataKey( devicesESID, deviceEntityKeyId );
            EntityDataKey participantEDK = new EntityDataKey( participantsESID, participantEKID );
            EntityDataKey studyEDK = new EntityDataKey( studyESID, studyEKID );

            ListMultimap<UUID, DataEdge> associations = ArrayListMultimap.create();

            Map<UUID, Set<Object>> usedByEntity = ImmutableMap
                    .of( edmCacheManager.getPropertyTypeId( STRING_ID_FQN ), Sets.newHashSet( UUID.randomUUID() ) );

            associations.put( usedByESID, new DataEdge( deviceEDK, participantEDK, usedByEntity ) );
            associations.put( usedByESID, new DataEdge( deviceEDK, studyEDK, usedByEntity ) );

            dataApi.createAssociations( associations );

            logger.info(
                    "datasource registered" + ORG_STUDY_PARTICIPANT_DATASOURCE,
                    organizationId,
                    studyId,
                    participantId,
                    datasourceId
            );
            return deviceEntityKeyId;
        } catch ( Exception exception ) {
            logger.error(
                    "unable to register datasource" + ORG_STUDY_PARTICIPANT_DATASOURCE,
                    organizationId,
                    studyId,
                    participantId,
                    datasourceId
            );
            throw new RuntimeException( "unable to register datasource" );
        }
    }

    @Override
    public UUID registerDatasource(
            UUID organizationId,
            UUID studyId,
            String participantId,
            String datasourceId,
            Optional<Datasource> datasource ) {

        logger.info(
                "attempting to register data source" + ORG_STUDY_PARTICIPANT_DATASOURCE,
                organizationId,
                studyId,
                participantId,
                datasourceId
        );

        final boolean isKnownParticipant = isKnownParticipant( organizationId, studyId, participantId );
        if ( !isKnownParticipant ) {
            logger.error(
                    "unknown participant, unable to register datasource" + ORG_STUDY_PARTICIPANT_DATASOURCE,
                    organizationId,
                    studyId,
                    participantId,
                    datasourceId
            );
            throw new AccessDeniedException( "unknown participant, unable to register datasource" );
        }

        final UUID deviceEKID = getDeviceEntityKeyId( organizationId, studyId, participantId, datasourceId );
        if ( deviceEKID != null ) {
            logger.info(
                    "datasource is registered" + ORG_STUDY_PARTICIPANT_DATASOURCE,
                    organizationId,
                    studyId,
                    participantId,
                    datasourceId
            );
            return deviceEKID;
        }

        return registerDatasourceHelper( organizationId, studyId, participantId, datasourceId, datasource );
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

        logger.info( "getting participation status" + ORG_STUDY_PARTICIPANT, organizationId, studyId, participantId );

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
                    "participant not found"
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

            logger.info(
                    "participation status = {}" + ORG_STUDY_PARTICIPANT,
                    status,
                    organizationId,
                    studyId,
                    participantId
            );

            return status;
        } catch ( Exception exception ) {
            logger.error(
                    "unable to get participation status, returning {}" + ORG_STUDY_PARTICIPANT,
                    ParticipationStatus.UNKNOWN,
                    organizationId,
                    studyId,
                    participantId,
                    exception
            );
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
    public Set<UUID> getStudyParticipants( UUID organizationId, UUID studyId ) {
        if ( organizationId != null ) {
            Map<UUID, Map<String, UUID>> participants = scheduledTasksManager.getStudyParticipantsByOrg()
                    .getOrDefault( organizationId, Map.of() );

            return Sets.newHashSet( participants.getOrDefault( studyId, Map.of() ).values() );
        }

        return Sets
                .newHashSet( scheduledTasksManager.getStudyParticipants().getOrDefault( studyId, Map.of() ).values() );
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
