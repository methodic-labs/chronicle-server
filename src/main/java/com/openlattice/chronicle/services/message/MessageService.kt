package com.openlattice.chronicle.services.message

import com.google.common.collect.ArrayListMultimap
import com.google.common.collect.ImmutableSet
import com.google.common.collect.ListMultimap
import com.openlattice.chronicle.constants.EdmConstants
import com.openlattice.chronicle.data.MessageDetails
import com.openlattice.chronicle.constants.MessageOutcome
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
        for(outcome in outcomes) {
            try {
                recordMessageSent(organizationId, outcome)
                logger.info("Recorded notification {} sent to participant {}", outcome.sid, outcome.participantId)
            } catch (e: ExecutionException) {
                logger.error("Unable to record notification sent for SID {}", outcome.sid, e)
            }
        }
    }

    @Throws(ExecutionException::class)
    fun recordMessageSent(organizationId: UUID?, messageOutcome: MessageOutcome) {
        val apiClient = apiCacheManager.prodApiClientCache[ApiClient::class.java]
        val dataApi = apiClient.dataApi

        // entity set ids
        val participantES = ChronicleServerUtil.getParticipantEntitySetName(messageOutcome.studyId)
        val coreAppConfig = entitySetIdsManager
            .getChronicleAppConfig(organizationId)
        val legacyAppConfig = entitySetIdsManager
            .getLegacyChronicleAppConfig(participantES)

        val messageESID = coreAppConfig.messagesEntitySetId
        val sentToESID = legacyAppConfig.sentToEntitySetId
        val participantsESID = coreAppConfig.participantEntitySetId

        val participantEKID =
            enrollmentManager.getParticipantEntityKeyId(organizationId, messageOutcome.studyId, messageOutcome.participantId)

        val entities: ListMultimap<UUID, Map<UUID, Set<Any>>> = ArrayListMultimap.create()
        val associations: ListMultimap<UUID, DataAssociation> = ArrayListMultimap.create()

        val messageEntity = mapOf(
            edmCacheManager.getPropertyTypeId(EdmConstants.TYPE_FQN) to ImmutableSet.of<Any>(messageOutcome.messageType),
            edmCacheManager.getPropertyTypeId(EdmConstants.OL_ID_FQN) to ImmutableSet.of<Any>(messageOutcome.sid),
            edmCacheManager.getPropertyTypeId(EdmConstants.DATE_TIME_FQN) to ImmutableSet.of<Any>(messageOutcome.dateTime),
            edmCacheManager.getPropertyTypeId(EdmConstants.DELIVERED_FQN) to ImmutableSet.of<Any>(messageOutcome.isSuccess)
        )
        entities.put(messageESID, messageEntity)

        val sentToEntity = mapOf(
            edmCacheManager.getPropertyTypeId(EdmConstants.DATE_TIME_FQN) to ImmutableSet.of<Any>(messageOutcome.dateTime)
        )

        associations.put(
            sentToESID,
            DataAssociation(
                messageESID,
                Optional.of(0),
                Optional.empty(),
                participantsESID,
                Optional.empty(),
                Optional.of(participantEKID),
                sentToEntity
            )
        )
        val dataGraph = DataGraph(entities, associations)
        dataApi.createEntityAndAssociationData(dataGraph)
    }
}