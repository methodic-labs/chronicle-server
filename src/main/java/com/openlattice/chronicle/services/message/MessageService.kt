package com.openlattice.chronicle.services.message

import com.google.common.collect.ArrayListMultimap
import com.google.common.collect.ImmutableSet
import com.google.common.collect.ListMultimap
import com.openlattice.chronicle.constants.EdmConstants
import com.openlattice.chronicle.data.MessageDetails
import com.openlattice.chronicle.data.MessageOutcome
import com.openlattice.chronicle.services.ApiCacheManager
import com.openlattice.chronicle.services.edm.EdmCacheManager
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.services.entitysets.EntitySetIdsManager
import com.openlattice.chronicle.services.twilio.TwilioManager
import com.openlattice.chronicle.util.ChronicleServerUtil
import com.openlattice.client.ApiClient
import com.openlattice.data.DataAssociation
import com.openlattice.data.DataGraph
import org.slf4j.LoggerFactory
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.ExecutionException

/**
 * @author toddbergman &lt;todd@openlattice.com&gt;
 */
open class MessageService(
    private val apiCacheManager: ApiCacheManager,
    private val edmCacheManager: EdmCacheManager,
    private val enrollmentManager: EnrollmentManager,
    private val entitySetIdsManager: EntitySetIdsManager,
    private val twilioManager: TwilioManager
) : MessageManager {

    companion object {
        protected val logger = LoggerFactory.getLogger(MessageService::class.java)
    }

    override fun sendMessages(organizationId: UUID, messageDetailsList: List<MessageDetails>) {
        val outcomes = twilioManager.sendMessages(messageDetailsList)
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

        val entities: ListMultimap<UUID, Map<UUID, Set<Any>>> = ArrayListMultimap.create();
        val associations: ListMultimap<UUID, DataAssociation> = ArrayListMultimap.create()

        messageOutcomes.forEachIndexed { index, messageOutcome ->
            val participantEKID =
                enrollmentManager.getParticipantEntityKeyId(organizationId, messageOutcome.studyId, messageOutcome.participantId)

            val messageEntity = mapOf(
                edmCacheManager.getPropertyTypeId(EdmConstants.TYPE_FQN) to setOf(messageOutcome.messageType),
                edmCacheManager.getPropertyTypeId(EdmConstants.OL_ID_FQN) to setOf(messageOutcome.sid),
                edmCacheManager.getPropertyTypeId(EdmConstants.DATE_TIME_FQN) to setOf(messageOutcome.dateTime),
                edmCacheManager.getPropertyTypeId(EdmConstants.DELIVERED_FQN) to setOf(messageOutcome.isSuccess)
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
                """)
            }
        }
        catch (e: ExecutionException) {
            logger.info("Failed to record messages", e);
            for (messageOutcome in messageOutcomes) {
                logger.info(
                    """
                    Unable to record message ${messageOutcome.sid}
                    sent to participant ${messageOutcome.participantId}
                    for study ${messageOutcome.studyId}
                    in organization $organizationId
                """
                )
            }
        }
    }
}