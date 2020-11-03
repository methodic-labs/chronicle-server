package com.openlattice.chronicle.services.delete;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.openlattice.chronicle.data.DeleteType;
import com.openlattice.chronicle.services.ApiCacheManager;
import com.openlattice.chronicle.services.CommonTasksManager;
import com.openlattice.client.ApiClient;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.data.DataApi;
import com.openlattice.data.requests.FileType;
import com.openlattice.data.requests.NeighborEntityIds;
import com.openlattice.entitysets.EntitySetsApi;
import com.openlattice.search.SearchApi;
import com.openlattice.search.requests.EntityNeighborsFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.openlattice.chronicle.constants.AppComponent.CHRONICLE;
import static com.openlattice.chronicle.constants.AppComponent.CHRONICLE_DATA_COLLECTION;
import static com.openlattice.chronicle.constants.AppComponent.CHRONICLE_SURVEYS;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.ANSWER;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.APPDATA;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.DEVICE;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.PREPROCESSED_DATA;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.STUDIES;
import static com.openlattice.chronicle.constants.EdmConstants.ANSWERS_ES;
import static com.openlattice.chronicle.constants.EdmConstants.DATA_ES;
import static com.openlattice.chronicle.constants.EdmConstants.DEVICES_ES;
import static com.openlattice.chronicle.constants.EdmConstants.PREPROCESSED_DATA_ES;
import static com.openlattice.chronicle.constants.EdmConstants.STUDY_ES;
import static com.openlattice.chronicle.util.ChronicleServerUtil.checkNotNullUUIDs;
import static com.openlattice.edm.EdmConstants.ID_FQN;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public class DataDeletionService implements DataDeletionManager {
    protected static final Logger logger = LoggerFactory.getLogger( DataDeletionService.class );

    private final ApiCacheManager    apiCacheManager;
    private final CommonTasksManager commonTasksManager;

    public DataDeletionService( ApiCacheManager apiCacheManager, CommonTasksManager commonTasksManager ) {
        this.apiCacheManager = apiCacheManager;
        this.commonTasksManager = commonTasksManager;

    }

    // TODO: write tests for this
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
            SearchApi userSearchApi = userApiClient.getSearchApi();
            DataApi userDataApi = userApiClient.getDataApi();
            EntitySetsApi userEntitySetsApi = userApiClient.getEntitySetsApi();

            // load api for actions authenticated by chronicle super user.
            // for pre-existing studies, only the chronicle user can have permissions to delete from the entity sets
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            DataApi chronicleDataApi = apiClient.getDataApi();

            // get required entity set ids
            UUID studiesESID = commonTasksManager.getEntitySetId( organizationId, CHRONICLE, STUDIES, STUDY_ES );
            UUID participantsESID = commonTasksManager.getParticipantEntitySetId( organizationId, studyId );
            checkNotNullUUIDs( Sets.newHashSet( studiesESID, participantsESID ) );

            // these entity set ids will be null if the respective app modules have not been installed for the organization
            UUID appDataESID = commonTasksManager
                    .getEntitySetId( organizationId, CHRONICLE_DATA_COLLECTION, APPDATA, DATA_ES );
            UUID preprocessedDataESID = commonTasksManager
                    .getEntitySetId( organizationId, CHRONICLE_DATA_COLLECTION, PREPROCESSED_DATA,
                            PREPROCESSED_DATA_ES );
            UUID devicesESID = commonTasksManager
                    .getEntitySetId( organizationId, CHRONICLE_DATA_COLLECTION, DEVICE, DEVICES_ES );
            UUID answersESID = commonTasksManager
                    .getEntitySetId( organizationId, CHRONICLE_SURVEYS, ANSWER, ANSWERS_ES );

            // ensure study exists
            UUID studyEntityKeyId = Preconditions
                    .checkNotNull( commonTasksManager.getStudyEntityKeyId( organizationId, studyId ),
                            "study must exist" );

            // get a set of all participants to remove:
            Set<UUID> participantsToRemove = new HashSet<>();
            if ( participantId.isPresent() ) {
                // if participantId: add to set
                UUID participantEntityKeyId = commonTasksManager
                        .getParticipantEntityKeyId( organizationId, studyId, participantId.get() );
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

            Set<UUID> srcNeighborSetIds = ImmutableSet.of( devicesESID, appDataESID, preprocessedDataESID);
            Set<UUID> dstNeighborSetIds = ImmutableSet.of( answersESID);

            Map<UUID, Set<UUID>> toDeleteEntitySetIdEntityKeyId = Maps.newHashMap();

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
                            logger.info( "Attempt to remove participant without data." );
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
            // outside app configs context, only chronicle super user can delete neighbors
            toDeleteEntitySetIdEntityKeyId
                    .forEach(
                            ( entitySetId, entityKeyId ) -> ( organizationId == null ? chronicleDataApi : userDataApi )
                                    .deleteEntities( entitySetId, entityKeyId, deleteType )
                    );

            // delete participants
            Integer deleted = userDataApi.deleteEntities( participantsESID, participantsToRemove, deleteType );
            logger.info( "Deleted {} entities for participant {}.", deleted, participantId );

            // delete study if no participantId is specified
            if ( participantId.isEmpty() ) {
                // delete participant entity set if no app configs context
                if ( organizationId == null ) {
                    userEntitySetsApi.deleteEntitySet( participantsESID );
                    logger.info( "Deleted participant dataset for study {}.", studyId );
                }
                userDataApi.deleteEntities( studiesESID,
                        ImmutableSet.of( studyEntityKeyId ),
                        deleteType );

                if ( organizationId == null ) {
                    logger.info( "Deleted study {} from global studies dataset.", studyId );
                } else {
                    logger.info( "deleted study {} from org {}", studyId, organizationId );
                }
            }

        } catch ( Exception e ) {
            String errorMsg = "failed to delete participant data";
            logger.error( errorMsg, e );
            throw new RuntimeException( errorMsg );
        }

    }

    @Override
    public void deleteParticipantAndAllNeighbors(
            UUID organizationId, UUID studyId, String participantId, DeleteType deleteType, String token ) {
        com.openlattice.data.DeleteType deleteTypeTransformed = com.openlattice.data.DeleteType
                .valueOf( deleteType.toString() );
        deleteStudyData( organizationId, studyId, Optional.of( participantId ), deleteTypeTransformed, token );
        logger.info( "Successfully removed a participant from {}", studyId );
    }

    @Override
    public void deleteStudyAndAllNeighbors(
            UUID organizationId, UUID studyId, DeleteType deleteType, String token ) {

        com.openlattice.data.DeleteType deleteTypeTransformed = com.openlattice.data.DeleteType
                .valueOf( deleteType.toString() );
        deleteStudyData( organizationId, studyId, Optional.empty(), deleteTypeTransformed, token );
        logger.info( "Successfully removed study {}", studyId );
    }
}
