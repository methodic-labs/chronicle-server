package com.openlattice.chronicle.services.download;

import com.google.common.collect.ImmutableSet;
import com.openlattice.chronicle.constants.*;
import com.openlattice.chronicle.data.ChronicleCoreAppConfig;
import com.openlattice.chronicle.data.ChronicleDataCollectionAppConfig;
import com.openlattice.chronicle.data.EntitySetIdGraph;
import com.openlattice.chronicle.services.edm.EdmCacheManager;
import com.openlattice.chronicle.services.entitysets.EntitySetIdsManager;
import com.openlattice.client.ApiClient;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.edm.set.EntitySetPropertyMetadata;
import com.openlattice.entitysets.EntitySetsApi;
import com.openlattice.graph.GraphApi;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.openlattice.chronicle.constants.EdmConstants.DATE_LOGGED_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.STRING_ID_FQN;
import static com.openlattice.chronicle.constants.ParticipantDataType.PREPROCESSED;
import static com.openlattice.chronicle.constants.ParticipantDataType.RAW_DATA;
import static com.openlattice.chronicle.constants.ParticipantDataType.USAGE_DATA;
import static com.openlattice.chronicle.util.ChronicleServerUtil.getParticipantEntitySetName;
import static com.openlattice.edm.EdmConstants.ID_FQN;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public class DataDownloadService implements DataDownloadManager {
    protected static final Logger logger = LoggerFactory.getLogger( DataDownloadService.class );

    private final EntitySetIdsManager entitySetIdsManager;
    private final EdmCacheManager     edmCacheManager;

    public DataDownloadService( EntitySetIdsManager entitySetIdsManager, EdmCacheManager edmCacheManager ) {

        this.entitySetIdsManager = entitySetIdsManager;
        this.edmCacheManager = edmCacheManager;
    }

    private ParticipantDataIterable getParticipantDataHelper(
            UUID participantEKID,
            EntitySetIdGraph entitySetIdGraph,
            Set<FullQualifiedName> srcPropertiesToExclude,
            Set<FullQualifiedName> edgePropertiesToExclude,
            String token ) {

        try {
            ApiClient apiClient = new ApiClient( RetrofitFactory.Environment.PROD_INTEGRATION, () -> token );
            EntitySetsApi entitySetsApi = apiClient.getEntitySetsApi();
            GraphApi graphApi = apiClient.getGraphApi();

            // get entity sets property metadata
            UUID srcESID = entitySetIdGraph.getSrcEntitySetId();
            UUID edgeESID = entitySetIdGraph.getEdgeEntitySetId();

            Map<UUID, Map<UUID, EntitySetPropertyMetadata>> meta =
                    entitySetsApi.getPropertyMetadataForEntitySets( ImmutableSet.of( srcESID, edgeESID ) );

            Map<UUID, EntitySetPropertyMetadata> sourceMeta = meta.get( srcESID );
            Map<UUID, EntitySetPropertyMetadata> edgeMeta = meta.get( edgeESID );

            return new ParticipantDataIterable(
                    new ParticipantDataIterable.NeighborPageSupplier(
                            edmCacheManager,
                            graphApi,
                            entitySetIdGraph,
                            srcPropertiesToExclude,
                            edgePropertiesToExclude,
                            sourceMeta,
                            edgeMeta,
                            participantEKID
                    )
            );

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
    public ParticipantDataIterable getAllPreprocessedParticipantData(
            UUID organizationId,
            UUID studyId,
            UUID participatedInEntityKeyId,
            String token ) {

        EntitySetIdGraph entitySetIdGraph = getEntitySetIdGraph( organizationId, studyId, PREPROCESSED );

        Set<FullQualifiedName> srcPropertiesToExclude = ImmutableSet.of( STRING_ID_FQN, ID_FQN );
        Set<FullQualifiedName> edgePropertiesToExclude = ImmutableSet.of( STRING_ID_FQN, DATE_LOGGED_FQN, ID_FQN );

        return getParticipantDataHelper(
                participatedInEntityKeyId,
                entitySetIdGraph,
                srcPropertiesToExclude,
                edgePropertiesToExclude,
                token
        );
    }

    @Override
    public ParticipantDataIterable getAllParticipantData(
            UUID organizationId,
            UUID studyId,
            UUID participantEntityKeyId,
            String token ) {

        EntitySetIdGraph entitySetIdGraph = getEntitySetIdGraph( organizationId, studyId, RAW_DATA );

        Set<FullQualifiedName> srcPropertiesToExclude = ImmutableSet.of( STRING_ID_FQN, ID_FQN );
        Set<FullQualifiedName> edgePropertiesToExclude = ImmutableSet.of( STRING_ID_FQN, DATE_LOGGED_FQN, ID_FQN );

        return getParticipantDataHelper(
                participantEntityKeyId,
                entitySetIdGraph,
                srcPropertiesToExclude,
                edgePropertiesToExclude,
                token
        );
    }

    @Override
    public ParticipantDataIterable getAllParticipantAppsUsageData(
            UUID organizationId,
            UUID studyId,
            UUID participantEntityKeyId,
            String token ) {

        EntitySetIdGraph entitySetIdGraph = getEntitySetIdGraph( organizationId, studyId, USAGE_DATA );

        Set<FullQualifiedName> srcPropertiesToExclude = ImmutableSet.of( STRING_ID_FQN, ID_FQN );
        Set<FullQualifiedName> edgePropertiesToExclude = ImmutableSet.of( STRING_ID_FQN, ID_FQN );

        return getParticipantDataHelper(
                participantEntityKeyId,
                entitySetIdGraph,
                srcPropertiesToExclude,
                edgePropertiesToExclude,
                token
        );
    }

    private EntitySetIdGraph getEntitySetIdGraph(
            UUID organizationId,
            UUID studyId,
            ParticipantDataType participantDataType ) {

        ChronicleCoreAppConfig coreAppConfig = entitySetIdsManager
                .getChronicleAppConfig( organizationId, getParticipantEntitySetName( studyId ) );
        ChronicleDataCollectionAppConfig dataCollectionAppConfig = entitySetIdsManager
                .getChronicleDataCollectionAppConfig( organizationId );

        UUID participantESID = coreAppConfig.getParticipantEntitySetId();

        UUID dataESID;
        UUID edgeESID;

        switch ( participantDataType ) {
            case RAW_DATA:
                dataESID = dataCollectionAppConfig.getAppDataEntitySetId();
                edgeESID = dataCollectionAppConfig.getRecordedByEntitySetId();
                break;
            case USAGE_DATA:
                dataESID = dataCollectionAppConfig.getUserAppsEntitySetId();
                edgeESID = dataCollectionAppConfig.getUsedByEntitySetId();
                break;
            default:
                dataESID = dataCollectionAppConfig.getPreprocessedDataEntitySetId();
                edgeESID = dataCollectionAppConfig.getRecordedByEntitySetId();
        }

        return new EntitySetIdGraph( dataESID, edgeESID, participantESID );
    }
}
