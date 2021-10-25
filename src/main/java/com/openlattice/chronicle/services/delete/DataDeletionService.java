package com.openlattice.chronicle.services.delete;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.PermissionsApi;
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
import com.openlattice.controllers.exceptions.ForbiddenException;
import com.openlattice.data.DataApi;
import com.openlattice.data.DeleteType;
import com.openlattice.entitysets.EntitySetsApi;
import com.openlattice.search.SearchApi;
import com.openlattice.search.requests.EntityNeighborsFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.openlattice.chronicle.util.ChronicleServerUtil.getParticipantEntitySetName;

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

    // get a set of all participants to remove:
    private Set<UUID> getParticipantsToDelete(
            UUID organizationId,
            UUID studyId,
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
        return enrollmentManager.getStudyParticipants( organizationId, studyId );
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
            UUID submissionESID = surveysAppConfig.getSubmissionEntitySetId();
            UUID metadataESID = coreAppConfig.getMetadataEntitySetId();

            // user needs OWNER on all entity sets in order to delete
            Set<UUID> allEntitySetIds = Sets.newHashSet( Iterables.concat( coreAppConfig.getAllEntitySetIds(),
                    dataCollectionAppConfig.getAllEntitySetIds(),
                    surveysAppConfig.getAllEntitySetIds() ) );
            ensureUserCanDeleteData( allEntitySetIds, userApiClient.getPermissionsApi() );

            Set<UUID> participantsToDelete = getParticipantsToDelete( organizationId, studyId, participantId );

            /*
             * Since the data collection and surveys chronicle components are optional, entity set ids will be null if the
             * corresponding apps have not been installed for organization, thus we have to filter out the null values
             */
            Set<UUID> srcEntitySetIds = Sets
                    .filter( Sets.newHashSet( devicesESID, appDataESID, preprocessedDataESID ), Objects::nonNull );
            Set<UUID> dstEntitySetIds = Sets.filter( Sets.newHashSet( answersESID, submissionESID, metadataESID ),
                    Objects::nonNull );

            if ( !participantsToDelete.isEmpty() ) {
                dataApi.deleteEntitiesAndNeighbors( participantsESID, new EntityNeighborsFilter(
                        participantsToDelete,
                        Optional.of( srcEntitySetIds ),
                        Optional.of( dstEntitySetIds ),
                        Optional.empty()
                ), deleteType );

                logger.info( "Deleted {} participants from study {} in org {}.",
                        participantsToDelete.size(),
                        studyId,
                        organizationId );
            }

            if ( participantId.isPresent() ) {
                return;
            }

            // delete study if no participantId is specified
            dataApi.deleteEntities( studiesESID,
                    ImmutableSet.of( studyEntityKeyId ),
                    deleteType,
                    false );
            logger.info( "Deleted study {} from org {}", studyId, organizationId );

        } catch ( Exception e ) {
            String errorMsg = "failed to delete participant data";
            logger.error( errorMsg, e );
            throw new RuntimeException( errorMsg );
        }
    }

    private void ensureUserCanDeleteData( Set<UUID> entitySetIds, PermissionsApi permissionsApi ) {
        try {
            Set<AclKey> aclKeys = entitySetIds.stream().map( AclKey::new ).collect(
                    Collectors.toSet() );
            permissionsApi.getAcls( aclKeys );
        } catch ( Exception e ) {
            logger.error( "Authorization for deleting participant data failed" );
            throw new ForbiddenException( "insufficient permission to delete participant data" );
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

            // ensure that user has OWNER on participants entity set
            ensureUserCanDeleteData( ImmutableSet.of( participantsESID ), userApiClient.getPermissionsApi() );

            Set<UUID> participantsToDelete = getParticipantsToDelete( null, studyId, participantId );

            // delete participant neighbors
            Set<UUID> srcEntitySetIds = Sets.newHashSet( devicesESID, appDataESID, preprocessedDataESID );
            Set<UUID> dstEntitySetIds = Sets.newHashSet( answersESID );

            if ( !participantsToDelete.isEmpty() ) {
                chronicleDataApi.deleteEntitiesAndNeighbors( participantsESID,
                        new EntityNeighborsFilter(
                                participantsToDelete,
                                Optional.of( srcEntitySetIds ),
                                Optional.of( dstEntitySetIds ),
                                Optional.empty()
                        ), deleteType );
                logger.info( "Deleted {} participants from study {} ", participantsToDelete.size(), studyId );
            }

            if ( participantId.isPresent() ) {
                return;
            }

            // if no participant is specified, delete study
            chronicleDataApi.deleteEntities( studiesESID,
                    ImmutableSet.of( studyEntityKeyId ),
                    deleteType,
                    false );
            logger.info( "Deleted study {} from global studies dataset", studyId );

            userEntitySetsApi.deleteEntitySet( participantsESID, deleteType );
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
