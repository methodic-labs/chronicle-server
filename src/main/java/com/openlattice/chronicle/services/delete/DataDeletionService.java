package com.openlattice.chronicle.services.delete;

import com.dataloom.streams.StreamUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.openlattice.chronicle.data.ChronicleCoreAppConfig;
import com.openlattice.chronicle.data.ChronicleDataCollectionAppConfig;
import com.openlattice.chronicle.data.ChronicleDeleteType;
import com.openlattice.chronicle.data.ChronicleSurveysAppConfig;
import com.openlattice.chronicle.services.ApiCacheManager;
import com.openlattice.chronicle.services.edm.EdmCacheManager;
import com.openlattice.chronicle.services.enrollment.EnrollmentManager;
import com.openlattice.chronicle.services.entitysets.EntitySetIdsManager;
import com.openlattice.client.ApiClient;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.data.DataApi;
import com.openlattice.data.DeleteType;
import com.openlattice.data.requests.EntitySetSelection;
import com.openlattice.data.requests.FileType;
import com.openlattice.data.requests.NeighborEntityIds;
import com.openlattice.entitysets.EntitySetsApi;
import com.openlattice.search.SearchApi;
import com.openlattice.search.requests.EntityNeighborsFilter;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.openlattice.chronicle.constants.EdmConstants.PERSON_ID_FQN;
import static com.openlattice.chronicle.util.ChronicleServerUtil.getFirstUUIDOrNull;
import static com.openlattice.chronicle.util.ChronicleServerUtil.getParticipantEntitySetName;
import static com.openlattice.edm.EdmConstants.ID_FQN;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public class DataDeletionService implements DataDeletionManager {
    protected static final Logger logger = LoggerFactory.getLogger( DataDeletionService.class );

    private final ApiCacheManager     apiCacheManager;
    private final EnrollmentManager   enrollmentManager;
    private final EntitySetIdsManager entitySetIdsManager;
    private final EdmCacheManager     edmCacheManager;

    public DataDeletionService(
            EdmCacheManager edmCacheManager,
            ApiCacheManager apiCacheManager,
            EntitySetIdsManager entitySetIdsManager,
            EnrollmentManager enrollmentManager ) {

        this.edmCacheManager = edmCacheManager;
        this.apiCacheManager = apiCacheManager;
        this.enrollmentManager = enrollmentManager;
        this.entitySetIdsManager = entitySetIdsManager;
    }

    /*
     * Delete participant neighbors
     * WARNING: Care must be taken to not delete entities that neighbor other participants/neighbors.
     * The entities in srcEntitySetIds and dstEntitySetIds MUST have one-to-one mapping with participant.
     */
    private void deleteParticipantNeighbors(
            DataApi dataApi,
            SearchApi searchApi,
            Set<UUID> srcEntitySetIds,
            Set<UUID> dstEntitySetIds,
            Set<UUID> participantsToRemove,
            UUID participantsESID,
            DeleteType deleteType ) {

        Map<UUID, Set<UUID>> neighborEntityKeyIds = Maps.newHashMap();

        participantsToRemove.forEach(
                participantEntityKeyId -> {
                    // Get participant neighbors
                    Map<UUID, Map<UUID, SetMultimap<UUID, NeighborEntityIds>>> participantNeighbors = searchApi
                            .executeFilteredEntityNeighborIdsSearch(
                                    participantsESID,
                                    new EntityNeighborsFilter(
                                            ImmutableSet.of( participantEntityKeyId ),
                                            Optional.of( srcEntitySetIds ),
                                            Optional.of( dstEntitySetIds ),
                                            Optional.empty()
                                    )
                            );

                    if ( participantNeighbors.size() == 0 ) {
                        logger.info( "Attempt to remove participant without data." );
                    }

                    // fill Map<entitySetId, Set<entityKeyId>>
                    participantNeighbors
                            .getOrDefault( participantEntityKeyId, ImmutableMap.of() )
                            .forEach( ( edgeEntitySetId, edgeNeighbor ) -> {
                                edgeNeighbor.forEach( ( neighborEntitySetId, neighborEntityIds ) -> {
                                    neighborEntityKeyIds
                                            .computeIfAbsent( neighborEntitySetId, esid -> Sets.newHashSet() )
                                            .add( neighborEntityIds.getNeighborEntityKeyId() );
                                } );
                            } );
                }
        );

        // delete all neighbors
        // outside app configs context, only chronicle super user can delete neighbors
        neighborEntityKeyIds
                .forEach( ( entitySetId, entityKeyId ) -> dataApi
                        .deleteEntities( entitySetId, entityKeyId, deleteType, true )
                );
    }

    // get a set of all participants to remove:
    private Set<UUID> getParticipantsToDelete(
            DataApi dataApi,
            UUID organizationId,
            UUID studyId,
            UUID participantESID,
            Optional<String> participantId ) throws Exception {

        // specific participant
        if ( participantId.isPresent() ) {
            UUID participantEntityKeyId = enrollmentManager
                    .getParticipantEntityKeyId( organizationId, studyId, participantId.get() );
            if ( participantEntityKeyId == null ) {
                throw new Exception(
                        "unable to delete participant " + participantId.get() + ": participant does not exist." );
            }

            return ImmutableSet.of( participantEntityKeyId );
        }

        // no participant was specified, so remove all participants from entity set
        Iterable<SetMultimap<FullQualifiedName, Object>> entitySetData = dataApi.loadSelectedEntitySetData(
                participantESID,
                new EntitySetSelection( Optional
                        .of( ImmutableSet.of( edmCacheManager.getPropertyTypeId( PERSON_ID_FQN ) ) ) ),
                FileType.json
        );

        return StreamUtil.stream( entitySetData )
                .map( entity -> getFirstUUIDOrNull( Multimaps.asMap( entity ), ID_FQN ) )
                .filter( Objects::nonNull )
                .collect( Collectors.toSet() );
    }

    private void deleteStudyData(
            UUID organizationId,
            UUID studyId,
            Optional<String> participantId,
            com.openlattice.data.DeleteType deleteType,
            String userToken ) {
        try {
            // load api for actions authenticated by the user
            ApiClient userApiClient = new ApiClient( RetrofitFactory.Environment.PROD_INTEGRATION,
                    () -> userToken );
            SearchApi searchApi = userApiClient.getSearchApi();
            DataApi dataApi = userApiClient.getDataApi();

            // ensure study exists
            UUID studyEntityKeyId = Preconditions
                    .checkNotNull( enrollmentManager.getStudyEntityKeyId( organizationId, studyId ),
                            "study must exist" );

            // entity set ids
            ChronicleCoreAppConfig coreAppConfig = entitySetIdsManager
                    .getChronicleAppConfig( organizationId );
            ChronicleDataCollectionAppConfig dataCollectionAppConfig = entitySetIdsManager
                    .getChronicleDataCollectionAppConfig( organizationId );
            ChronicleSurveysAppConfig surveysAppConfig = entitySetIdsManager
                    .getChronicleSurveysAppConfig( organizationId );

            UUID studiesESID = coreAppConfig.getStudiesEntitySetId();
            UUID participantsESID = coreAppConfig.getParticipantEntitySetId();
            UUID appDataESID = dataCollectionAppConfig.getAppDataEntitySetId();
            UUID preprocessedDataESID = dataCollectionAppConfig.getPreprocessedDataEntitySetId();
            UUID devicesESID = dataCollectionAppConfig.getDeviceEntitySetId();
            UUID answersESID = surveysAppConfig.getAnswerEntitySetId();

            Set<UUID> participantsToDelete = getParticipantsToDelete( dataApi,
                    organizationId,
                    studyId,
                    participantsESID,
                    participantId );

            /*
             * Since the data collection and surveys chronicle components are optional, entity set ids will be null if the
             * corresponding apps have not been installed for organization, thus we have to filter out the null values
             */
            Set<UUID> srcEntitySetIds = Sets
                    .filter( Sets.newHashSet( devicesESID, appDataESID, preprocessedDataESID ), Objects::nonNull );
            Set<UUID> dstEntitySetIds = Sets.filter( Sets.newHashSet( answersESID ), Objects::nonNull );

            deleteParticipantNeighbors(
                    dataApi,
                    searchApi,
                    srcEntitySetIds,
                    dstEntitySetIds,
                    participantsToDelete,
                    participantsESID,
                    deleteType );

            // delete participants
            int deleted = dataApi.deleteEntities( participantsESID, participantsToDelete, deleteType, true );
            logger.info( "Deleted {} participants from study {} in org {}.", deleted, studyId, organizationId );

            // delete study if no participantId is specified
            if ( participantId.isPresent() ) {
                return;
            }

            dataApi.deleteEntities( studiesESID,
                    ImmutableSet.of( studyEntityKeyId ),
                    deleteType,
                    true );
            logger.info( "Deleted study {} from org {}", studyId, organizationId );

        } catch ( Exception e ) {
            String errorMsg = "failed to delete participant data";
            logger.error( errorMsg, e );
            throw new RuntimeException( errorMsg );
        }
    }

    private void legacyDeleteStudyData(
            UUID studyId,
            Optional<String> participantId,
            DeleteType deleteType,
            String jwtToken ) {

        try {
            // api for actions authenticated for user
            ApiClient userApiClient = new ApiClient( RetrofitFactory.Environment.PROD_INTEGRATION, () -> jwtToken );

            SearchApi userSearchApi = userApiClient.getSearchApi();
            DataApi userDataApi = userApiClient.getDataApi();
            EntitySetsApi userEntitySetsApi = userApiClient.getEntitySetsApi();

            // load api for actions authenticated by chronicle super user.
            // for legacy studies, only the chronicle super user has permissions to delete from the participant neighbor entity sets
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            DataApi chronicleDataApi = apiClient.getDataApi();

            // ensure study exists
            UUID studyEntityKeyId = Preconditions
                    .checkNotNull( enrollmentManager.getStudyEntityKeyId( null, studyId ),
                            "study must exist" );

            // entity set ids
            ChronicleCoreAppConfig coreAppConfig = entitySetIdsManager
                    .getLegacyChronicleAppConfig( getParticipantEntitySetName( studyId ) );
            ChronicleDataCollectionAppConfig dataCollectionAppConfig = entitySetIdsManager
                    .getLegacyChronicleDataCollectionAppConfig();
            ChronicleSurveysAppConfig surveysAppConfig = entitySetIdsManager.getLegacyChronicleSurveysAppConfig();

            UUID studiesESID = coreAppConfig.getStudiesEntitySetId();
            UUID participantsESID = coreAppConfig.getParticipantEntitySetId();
            UUID appDataESID = dataCollectionAppConfig.getAppDataEntitySetId();
            UUID preprocessedDataESID = dataCollectionAppConfig.getPreprocessedDataEntitySetId();
            UUID devicesESID = dataCollectionAppConfig.getDeviceEntitySetId();
            UUID answersESID = surveysAppConfig.getAnswerEntitySetId();

            Set<UUID> participantsToDelete = getParticipantsToDelete(
                    userDataApi,
                    null,
                    studyId,
                    participantsESID,
                    participantId );

            // delete participant neighbors
            Set<UUID> srcEntitySetIds = Sets.newHashSet( devicesESID, appDataESID, preprocessedDataESID );
            Set<UUID> dstEntitySetIds = Sets.newHashSet( answersESID );

            deleteParticipantNeighbors(
                    chronicleDataApi,
                    userSearchApi,
                    srcEntitySetIds,
                    dstEntitySetIds,
                    participantsToDelete,
                    participantsESID,
                    deleteType );

            // delete participants
            int deleted = userDataApi.deleteEntities( participantsESID, participantsToDelete, deleteType, true );
            logger.info( "Deleted {} participants from study {}.", deleted, studyId );

            // if no participant is specified, delete study
            if ( participantId.isPresent() ) {
                return;
            }

            userDataApi.deleteEntities( studiesESID,
                    ImmutableSet.of( studyEntityKeyId ),
                    deleteType,
                    true );
            logger.info( "Deleted study {} from global studies dataset", studyId );

            userEntitySetsApi.deleteEntitySet( participantsESID );
            logger.info( "Deleted participant dataset for study {}.", studyId );

        } catch ( Exception e ) {
            String errorMessage = "failed to delete participant data";
            logger.error( errorMessage, e );
            throw new RuntimeException( errorMessage );
        }
    }

    @Override
    public void deleteParticipantAndAllNeighbors(
            UUID organizationId,
            UUID studyId,
            String participantId,
            ChronicleDeleteType chronicleDeleteType,
            String token ) {

        Stopwatch stopwatch = Stopwatch.createStarted();

        // ChronicleDeleteType has exactly the same values as DeleteType and was added to the chronicle api module
        // to get around dependency issues in android app
        DeleteType deleteType = DeleteType.valueOf( chronicleDeleteType.toString() );

        if ( organizationId == null ) {
            logger.info( "Removing participant's {} data from study {}'s participants dataset",
                    participantId,
                    studyId );

            legacyDeleteStudyData( studyId, Optional.of( participantId ), deleteType, token );

            stopwatch.stop();
            logger.info( "Successfully removed participant {} and neighbors in {} seconds. studyId = {}",
                    participantId,
                    stopwatch.elapsed( TimeUnit.SECONDS ),
                    studyId );
        } else {
            logger.info( "Removing participant {} from org {}", participantId, organizationId );

            deleteStudyData( organizationId, studyId, Optional.of( participantId ), deleteType, token );

            stopwatch.stop();

            logger.info( "Successfully removed participant {} and its neighbors from org {} in {} seconds",
                    participantId,
                    organizationId,
                    stopwatch.elapsed( TimeUnit.SECONDS ) );
        }
    }

    @Override
    public void deleteStudyAndAllNeighbors(
            UUID organizationId, UUID studyId, ChronicleDeleteType chronicleDeleteType, String token ) {

        Stopwatch stopwatch = Stopwatch.createStarted();

        // ChronicleDeleteType has exactly the same values as DeleteType and was added to the chronicle api module
        // to get around dependency issues in android app
        DeleteType deleteType = DeleteType.valueOf( chronicleDeleteType.toString() );

        if ( organizationId == null ) {
            logger.info( "removing study {} from the global study entity set", studyId );

            legacyDeleteStudyData( studyId, Optional.empty(), deleteType, token );
        } else {
            logger.info( "removing study {} from org {}", studyId, organizationId );

            deleteStudyData( organizationId, studyId, Optional.empty(), deleteType, token );
        }

        stopwatch.stop();
        logger.info( "Successfully removed study {} and all its neighbors in {} seconds",
                studyId,
                stopwatch.elapsed( TimeUnit.SECONDS ) );
    }
}
