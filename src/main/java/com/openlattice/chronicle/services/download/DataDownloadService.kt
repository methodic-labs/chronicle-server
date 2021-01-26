package com.openlattice.chronicle.services.download

import com.google.common.collect.ImmutableSet
import com.openlattice.chronicle.constants.EdmConstants
import com.openlattice.chronicle.constants.ParticipantDataType
import com.openlattice.chronicle.data.EntitySetIdGraph
import com.openlattice.chronicle.services.download.ParticipantDataIterable.NeighborPageSupplier
import com.openlattice.chronicle.services.edm.EdmCacheManager
import com.openlattice.chronicle.services.entitysets.EntitySetIdsManager
import com.openlattice.chronicle.util.ChronicleServerUtil
import com.openlattice.client.ApiClient
import com.openlattice.client.RetrofitFactory
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.lang.Exception
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class DataDownloadService(private val entitySetIdsManager: EntitySetIdsManager, private val edmCacheManager: EdmCacheManager) : DataDownloadManager {
    companion object {
        private val logger = LoggerFactory.getLogger(DataDownloadService::class.java)
    }

    private fun getParticipantDataHelper(
            participantEKID: UUID?,
            entitySetIdGraph: EntitySetIdGraph,
            srcPropertiesToExclude: Set<FullQualifiedName>,
            edgePropertiesToExclude: Set<FullQualifiedName>,
            token: String?): Iterable<Map<String, Set<Any>>> {

        return try {
            val apiClient = ApiClient(RetrofitFactory.Environment.PROD_INTEGRATION) { token }
            val entitySetsApi = apiClient.entitySetsApi
            val graphApi = apiClient.graphApi

            // get entity sets property metadata
            val srcESID = entitySetIdGraph.srcEntitySetId
            val edgeESID = entitySetIdGraph.edgeEntitySetId
            val meta = entitySetsApi.getPropertyMetadataForEntitySets(ImmutableSet.of(srcESID, edgeESID))
            val sourceMeta = meta[srcESID]
            val edgeMeta = meta[edgeESID]
            ParticipantDataIterable(
                    NeighborPageSupplier(
                            edmCacheManager,
                            graphApi,
                            entitySetIdGraph,
                            srcPropertiesToExclude,
                            edgePropertiesToExclude,
                            sourceMeta!!,
                            edgeMeta!!,
                            participantEKID!!
                    )
            )
        } catch (e: Exception) {
            // since the response is meant to be a file download, returning "null" will respond with 200 and return
            // an empty file, which is not what we want. the request should not "succeed" when something goes wrong
            // internally. additionally, it doesn't seem right for the request to return a stacktrace. instead,
            // catching all exceptions and throwing a general exception here will result in a failed request with
            // a simple error message to indicate something went wrong during the file download.
            val errorMsg = "failed to download participant data"
            logger.error(errorMsg, e)
            throw RuntimeException(errorMsg)
        }
    }

    override fun getAllPreprocessedParticipantData(
            organizationId: UUID?,
            studyId: UUID?,
            participantEntityId: UUID?,
            token: String?): Iterable<Map<String, Set<Any>>> {
        val entitySetIdGraph = getEntitySetIdGraph(organizationId, studyId, ParticipantDataType.PREPROCESSED)
        val srcPropertiesToExclude: Set<FullQualifiedName> = ImmutableSet.of(EdmConstants.STRING_ID_FQN, com.openlattice.edm.EdmConstants.ID_FQN)
        val edgePropertiesToExclude: Set<FullQualifiedName> = ImmutableSet.of(EdmConstants.STRING_ID_FQN, EdmConstants.DATE_LOGGED_FQN, com.openlattice.edm.EdmConstants.ID_FQN)

        return getParticipantDataHelper(
                participantEntityId,
                entitySetIdGraph,
                srcPropertiesToExclude,
                edgePropertiesToExclude,
                token
        )
    }

    override fun getAllParticipantData(
            organizationId: UUID?,
            studyId: UUID?,
            participantEntityId: UUID?,
            token: String?): Iterable<Map<String, Set<Any>>> {
        val entitySetIdGraph = getEntitySetIdGraph(organizationId, studyId, ParticipantDataType.RAW_DATA)
        val srcPropertiesToExclude: Set<FullQualifiedName> = ImmutableSet.of(EdmConstants.STRING_ID_FQN, com.openlattice.edm.EdmConstants.ID_FQN)
        val edgePropertiesToExclude: Set<FullQualifiedName> = ImmutableSet.of(EdmConstants.STRING_ID_FQN, EdmConstants.DATE_LOGGED_FQN, com.openlattice.edm.EdmConstants.ID_FQN)

        return getParticipantDataHelper(
                participantEntityId,
                entitySetIdGraph,
                srcPropertiesToExclude,
                edgePropertiesToExclude,
                token
        )
    }

    override fun getAllParticipantAppsUsageData(
            organizationId: UUID?,
            studyId: UUID?,
            participantEntityId: UUID?,
            token: String?): Iterable<Map<String, Set<Any>>> {
        val entitySetIdGraph = getEntitySetIdGraph(organizationId, studyId, ParticipantDataType.USAGE_DATA)
        val srcPropertiesToExclude: Set<FullQualifiedName> = ImmutableSet.of(EdmConstants.STRING_ID_FQN, com.openlattice.edm.EdmConstants.ID_FQN)
        val edgePropertiesToExclude: Set<FullQualifiedName> = ImmutableSet.of(EdmConstants.STRING_ID_FQN, com.openlattice.edm.EdmConstants.ID_FQN)

        return getParticipantDataHelper(
                participantEntityId,
                entitySetIdGraph,
                srcPropertiesToExclude,
                edgePropertiesToExclude,
                token
        )
    }

    private fun getEntitySetIdGraph(
            organizationId: UUID?,
            studyId: UUID?,
            participantDataType: ParticipantDataType): EntitySetIdGraph {
        val coreAppConfig = entitySetIdsManager
                .getChronicleAppConfig(organizationId, ChronicleServerUtil.getParticipantEntitySetName(studyId))
        val dataCollectionAppConfig = entitySetIdsManager
                .getChronicleDataCollectionAppConfig(organizationId)
        val participantESID = coreAppConfig.participantEntitySetId
        val dataESID: UUID
        val edgeESID: UUID
        when (participantDataType) {
            ParticipantDataType.RAW_DATA -> {
                dataESID = dataCollectionAppConfig.appDataEntitySetId
                edgeESID = dataCollectionAppConfig.recordedByEntitySetId
            }
            ParticipantDataType.USAGE_DATA -> {
                dataESID = dataCollectionAppConfig.userAppsEntitySetId
                edgeESID = dataCollectionAppConfig.usedByEntitySetId
            }
            else -> {
                dataESID = dataCollectionAppConfig.preprocessedDataEntitySetId
                edgeESID = dataCollectionAppConfig.recordedByEntitySetId
            }
        }
        return EntitySetIdGraph(dataESID, edgeESID, participantESID)
    }
}