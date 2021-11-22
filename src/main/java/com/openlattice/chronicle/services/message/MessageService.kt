package com.openlattice.chronicle.services.message

import com.google.common.collect.*
import com.openlattice.chronicle.constants.EdmConstants
import com.openlattice.chronicle.data.MessageDetails
import com.openlattice.chronicle.data.MessageOutcome
import com.openlattice.chronicle.services.ApiCacheManager
import com.openlattice.chronicle.services.edm.EdmCacheManager
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.services.entitysets.EntitySetIdsManager
import com.openlattice.chronicle.services.twilio.TwilioManager
import com.openlattice.client.ApiClient
import com.openlattice.data.DataAssociation
import com.openlattice.data.DataGraph
import com.openlattice.data.PropertyUpdateType
import com.openlattice.data.UpdateType
import com.openlattice.search.requests.SearchTerm
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.ExecutionException

/**
 * @author todd bergman &lt;todd@openlattice.com&gt;
 */
open class MessageService(
        private val apiCacheManager: ApiCacheManager,
        private val edmCacheManager: EdmCacheManager,
        private val enrollmentManager: EnrollmentManager,
        private val entitySetIdsManager: EntitySetIdsManager,
        private val twilioManager: TwilioManager
) : MessageManager {

    companion object {
        protected val logger: Logger = LoggerFactory.getLogger(MessageService::class.java)
        private const val MAX_FAILURE_ATTEMPTS = 5

    }

    open fun getSearchTermString(propertyTypeId: UUID, searchString: String): String {
        return "entity.$propertyTypeId:\"$searchString\""
    }

    @Throws(ExecutionException::class)
    override fun trackUndeliveredMessage(organizationId: UUID, messageSid: String) {
        try {
            val apiClient = apiCacheManager.prodApiClientCache[ApiClient::class.java]
            val dataApi = apiClient.dataApi
            val searchApi = apiClient.searchApi
            val coreAppConfig = entitySetIdsManager.getChronicleAppConfig(organizationId)

            // entity set ids
            val messageESID = coreAppConfig.messagesEntitySetId
            val searchTerm = SearchTerm( getSearchTermString(
                edmCacheManager.getPropertyTypeId(EdmConstants.OL_ID_FQN),
                messageSid
            ), 0, 1)

            var numAttempts = 0
            var entity: Map<FullQualifiedName, Set<Any>>? = null
            while (entity == null && numAttempts <= MAX_FAILURE_ATTEMPTS) {
                val searchResult = searchApi.executeEntitySetDataQuery( messageESID, searchTerm )
                if (searchResult != null && searchResult.hits.isNotEmpty()) {
                    entity = searchResult.hits[0]
                }
                try {
                    Thread.sleep(1000) // if message sid not indexed in elasticsearch yet, wait a second and retry
                } catch (e: InterruptedException) {
                }
                numAttempts++
            }
            if (entity != null) {
                val entityKeyId = UUID.fromString(
                        entity[EdmConstants.OL_EKID]!!.iterator().next().toString()
                    )
                val data = mapOf(
                    edmCacheManager.getPropertyTypeId(EdmConstants.DELIVERED_FQN) to setOf(false)
                )
                dataApi.updateEntitiesInEntitySet(
                    messageESID,
                    mapOf(entityKeyId to data),
                    UpdateType.PartialReplace,
                    PropertyUpdateType.Versioned
                )
            } else {
                logger.error("Unable to find failed message with SID $messageSid in search results")
            }
        } catch (e: ExecutionException) {
            logger.error("Unable to track undelivered message for message with SID $messageSid", e)
        }

    }

    override fun sendMessages(organizationId: UUID, messageDetailsList: List<MessageDetails>) {
        logger.info("preparing to send batch of ${messageDetailsList.size} messages to participants in organization $organizationId")
        val outcomes = twilioManager.sendMessages(organizationId, messageDetailsList)
        recordMessagesSent(organizationId, outcomes)
    }

    @Throws(ExecutionException::class)
    fun recordMessagesSent(organizationId: UUID?, messageOutcomes: List<MessageOutcome>) {
        val apiClient = apiCacheManager.prodApiClientCache[ApiClient::class.java]
        val dataApi = apiClient.dataApi
        val coreAppConfig = entitySetIdsManager.getChronicleAppConfig(organizationId)

        // entity set ids
        val messageESID = coreAppConfig.messagesEntitySetId
        val sentToESID = coreAppConfig.sentToEntitySetId
        val participantsESID = coreAppConfig.participantEntitySetId

        val entities: ListMultimap<UUID, Map<UUID, Set<Any>>> = ArrayListMultimap.create()
        val associations: ListMultimap<UUID, DataAssociation> = ArrayListMultimap.create()

        messageOutcomes.forEachIndexed { index, messageOutcome ->
            val participantEKID =
                    enrollmentManager.getParticipantEntityKeyId(organizationId, messageOutcome.studyId, messageOutcome.participantId)

            val messageEntity = mapOf(
                edmCacheManager.getPropertyTypeId(EdmConstants.TYPE_FQN) to ImmutableSet.of<Any>(messageOutcome.messageType),
                edmCacheManager.getPropertyTypeId(EdmConstants.OL_ID_FQN) to ImmutableSet.of<Any>(messageOutcome.sid),
                edmCacheManager.getPropertyTypeId(EdmConstants.DATE_TIME_FQN) to ImmutableSet.of<Any>(messageOutcome.dateTime),
                edmCacheManager.getPropertyTypeId(EdmConstants.DELIVERED_FQN) to ImmutableSet.of<Any>(messageOutcome.isSuccess)
            )

            entities.put(
                    messageESID,
                    messageEntity
            )

            val sentToEntity = mapOf(
                edmCacheManager.getPropertyTypeId(EdmConstants.DATE_TIME_FQN) to setOf(messageOutcome.dateTime)
            )

            associations.put(
                    sentToESID,
                    DataAssociation(
                            messageESID,
                            Optional.of(index),
                            Optional.empty(),
                            participantsESID,
                            Optional.empty(),
                            Optional.of(participantEKID),
                            sentToEntity
                    )
            )
        }
        val dataGraph = DataGraph(entities, associations)
        try {
            dataApi.createEntityAndAssociationData(dataGraph)
            for (messageOutcome in messageOutcomes) {
                logger.info("""
                    Recorded message ${messageOutcome.sid}
                    sent to participant ${messageOutcome.participantId}
                    for study ${messageOutcome.studyId}
                    in organization $organizationId
                """.trimIndent())
            }
        } catch (e: ExecutionException) {
            logger.info("Failed to record messages", e)
            for (messageOutcome in messageOutcomes) {
                logger.info("""
                    Unable to record message ${messageOutcome.sid}
                    sent to participant ${messageOutcome.participantId}
                    for study ${messageOutcome.studyId}
                    in organization $organizationId
                """.trimIndent())
            }
        }
    }
}