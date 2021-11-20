package com.openlattice.chronicle.services.download

import com.google.common.collect.ImmutableSet
import com.openlattice.chronicle.constants.AppComponent
import com.openlattice.chronicle.constants.EdmConstants.USER_FQN
import com.openlattice.chronicle.constants.OutputConstants
import com.openlattice.chronicle.constants.ParticipantDataType
import com.openlattice.chronicle.data.EntitySetIdGraph
import com.openlattice.chronicle.services.download.ParticipantDataIterable.NeighborPageSupplier
import com.openlattice.chronicle.services.edm.EdmCacheManager
import com.openlattice.chronicle.services.entitysets.EntitySetIdsManager
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
        const val RAW_DATA_SHOW_DEVICE_USER = "rawDataIndicateDeviceUser"
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

            val appSettings = if (organizationId != null) entitySetIdsManager.getOrgAppSettings(AppComponent.CHRONICLE_DATA_COLLECTION, organizationId) else mapOf()

            val srcPropertiesToInclude = DownloadTypePropertyTypeFqns.SRC.getValue(dataType).let {
                if (dataType == ParticipantDataType.RAW_DATA && appSettings.getOrDefault(RAW_DATA_SHOW_DEVICE_USER, false) as Boolean) {
                    it + linkedSetOf(USER_FQN)
                } else {
                    it
                }
            }
            val edgePropertiesToInclude = DownloadTypePropertyTypeFqns.EDGE.getValue(dataType)

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

    private fun getEntitySetIdGraph(
            organizationId: UUID?,
            studyId: UUID?,
            participantDataType: ParticipantDataType): EntitySetIdGraph {
        val entitySetsConfig = entitySetIdsManager.getEntitySetsConfig(organizationId, studyId, ImmutableSet.of(AppComponent.CHRONICLE_DATA_COLLECTION));

        val participantESID = entitySetsConfig.participantEntitySetId
        val dataESID: UUID
        val edgeESID: UUID
        when (participantDataType) {
            ParticipantDataType.RAW_DATA -> {
                dataESID = entitySetsConfig.appDataEntitySetId
                edgeESID = entitySetsConfig.recordedByEntitySetId
            }
            ParticipantDataType.USAGE_DATA -> {
                dataESID = entitySetsConfig.userAppsEntitySetId
                edgeESID = entitySetsConfig.usedByEntitySetId
            }
            else -> {
                dataESID = entitySetsConfig.preprocessedDataEntitySetId
                edgeESID = entitySetsConfig.recordedByEntitySetId
            }
        }
        return EntitySetIdGraph(dataESID, edgeESID, participantESID)
    }
}