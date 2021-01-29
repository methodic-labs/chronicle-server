package com.openlattice.chronicle.services.download

import com.google.common.collect.ImmutableSet
import com.openlattice.chronicle.constants.EdmConstants.*
import com.openlattice.chronicle.constants.OutputConstants
import com.openlattice.chronicle.constants.ParticipantDataType
import com.openlattice.chronicle.data.EntitySetIdGraph
import com.openlattice.chronicle.services.download.ParticipantDataIterable.NeighborPageSupplier
import com.openlattice.chronicle.services.edm.EdmCacheManager
import com.openlattice.chronicle.services.entitysets.EntitySetIdsManager
import com.openlattice.chronicle.util.ChronicleServerUtil
import com.openlattice.client.ApiClient
import com.openlattice.client.RetrofitFactory
import com.openlattice.edm.set.EntitySetPropertyMetadata
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class DataDownloadService(private val entitySetIdsManager: EntitySetIdsManager, private val edmCacheManager: EdmCacheManager) : DataDownloadManager {
    companion object {
        private val logger = LoggerFactory.getLogger(DataDownloadService::class.java)
    }

    private fun getParticipantDataHelper(
            organizationId: UUID?,
            studyId: UUID,
            participantEKID: UUID,
            dataType: ParticipantDataType,
            token: String?): Iterable<Map<String, Set<Any>>> {

        return try {
            val apiClient = ApiClient(RetrofitFactory.Environment.PROD_INTEGRATION) { token }
            val entitySetsApi = apiClient.entitySetsApi
            val graphApi = apiClient.graphApi

            val entitySetIdGraph = getEntitySetIdGraph(organizationId, studyId, dataType)

            // get entity sets property metadata
            val srcESID = entitySetIdGraph.srcEntitySetId
            val edgeESID = entitySetIdGraph.edgeEntitySetId
            val metadata = entitySetsApi.getPropertyMetadataForEntitySets(ImmutableSet.of(srcESID, edgeESID))
            val srcMetadata = metadata.getValue(srcESID)
            val edgeMetadata = metadata.getValue(edgeESID)

            val srcPropertiesToInclude = getSrcPropertiesToInclude(dataType)
            val edgePropertiesToInclude = getEdgePropertiesToInclude(dataType)

            val srcColumnTitles = getColumnTitles(srcPropertiesToInclude, srcMetadata, OutputConstants.APP_PREFIX)
            val edgeColumnTitles = getColumnTitles(edgePropertiesToInclude, edgeMetadata, OutputConstants.USER_PREFIX)

            ParticipantDataIterable(
                    srcColumnTitles + edgeColumnTitles,
                    NeighborPageSupplier(
                            edmCacheManager,
                            graphApi,
                            entitySetIdGraph,
                            srcPropertiesToInclude,
                            edgePropertiesToInclude,
                            srcMetadata,
                            edgeMetadata,
                            participantEKID
                    )
            )
        } catch (e: Exception) {
            // since the response is meant to be a file download, returning "null" will respond with 200 and return
            // an empty file, which is not what we want. the request should not "succeed" when something goes wrong
            // internally. additionally, it doesn't seem right for the request to return a stacktrace. instead,
            // catching all exceptions and throwing a general exception here will result in a failed request with
            // a simple error message to indicate something went wrong during the file download.
            logger.error("failed to download data for participant {}", participantEKID, e)
            throw RuntimeException("failed to download participant data")
        }
    }

    override fun getParticipantData(
            organizationId: UUID?,
            studyId: UUID,
            participantEntityId: UUID,
            dataType: ParticipantDataType,
            token: String?
    ): Iterable<Map<String, Set<Any>>> {

        return getParticipantDataHelper(
                organizationId,
                studyId,
                participantEntityId,
                dataType,
                token
        )
    }

    private fun getColumnTitles(propertyTypes: Set<FullQualifiedName>, metadata: Map<UUID, EntitySetPropertyMetadata>, prefix: String): List<String> {
        return propertyTypes.map {
            val propertyTypeId = edmCacheManager.getPropertyTypeId(it)
            prefix + metadata.getValue(propertyTypeId).title
        }
    }

    private fun getSrcPropertiesToInclude(dataType: ParticipantDataType): Set<FullQualifiedName> {
        return when (dataType) {
            ParticipantDataType.USAGE_DATA -> linkedSetOf(TITLE_FQN, FULL_NAME_FQN)
            ParticipantDataType.RAW_DATA -> linkedSetOf(
                    DATE_LOGGED_FQN,
                    TIMEZONE_FQN,
                    TITLE_FQN,
                    FULL_NAME_FQN,
                    RECORD_TYPE_FQN
            )
            ParticipantDataType.PREPROCESSED -> linkedSetOf(
                    NEW_APP_FQN,
                    TIMEZONE_FQN,
                    START_DATE_TIME_FQN,
                    GENERAL_END_TIME_FQN,
                    RECORD_TYPE_FQN,
                    TITLE_FQN,
                    FULL_NAME_FQN,
                    NEW_PERIOD_FQN,
                    DURATION_FQN,
                    WARNING_FQN
            )
        }
    }

    private fun getEdgePropertiesToInclude(dataType: ParticipantDataType): Set<FullQualifiedName> {
        return when (dataType) {
            ParticipantDataType.USAGE_DATA -> linkedSetOf(USER_FQN, DATE_TIME_FQN)
            ParticipantDataType.RAW_DATA -> linkedSetOf()
            ParticipantDataType.PREPROCESSED -> linkedSetOf()
        }
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