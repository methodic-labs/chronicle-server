package com.openlattice.chronicle.services.twilio

import com.openlattice.chronicle.api.ChronicleApi.*
import com.openlattice.chronicle.configuration.TwilioConfiguration
import com.openlattice.chronicle.data.MessageDetails
import com.openlattice.chronicle.data.MessageOutcome
import com.twilio.Twilio
import com.twilio.exception.ApiException
import com.twilio.rest.api.v2010.account.Message
import com.twilio.type.PhoneNumber
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.URI
import java.time.OffsetDateTime
import java.util.*

/**
 * @author toddbergman &lt;todd@openlattice.com&gt;
 */
open class TwilioService(configuration: TwilioConfiguration) :
    TwilioManager {
    companion object {
        protected val logger: Logger = LoggerFactory.getLogger(TwilioService::class.java)
    }

    init {
        Twilio.init(configuration.sid, configuration.token)
    }

    private val fromPhoneNumber: PhoneNumber = PhoneNumber(configuration.fromPhone)

    private fun sendMessage(organizationId: UUID, messageDetails: MessageDetails) :MessageOutcome {
        val messageText = "Chronicle device enrollment:  Please download app from your app store and click on ${messageDetails.url} to enroll your device."
        try {
            val message = Message
                .creator(PhoneNumber(messageDetails.phoneNumber), fromPhoneNumber, messageText)
                .setStatusCallback(URI.create(
                    "https://api.openlattice.com$BASE/$organizationId$MESSAGE_PATH$STATUS_PATH"))
                .create()
            logger.info(
                """
                    message sent to participant ${messageDetails.participantId}
                    for study ${messageDetails.studyId}
                    in organization $organizationId
                """.trimIndent()
            )
            return MessageOutcome(
                messageDetails.messageType,
                OffsetDateTime.now(),
                messageDetails.participantId,
                messageDetails.url,
                message.status != Message.Status.FAILED,
                message.sid,
                messageDetails.studyId
            )
        } catch (e: ApiException) {
            logger.error("""
                Unable to send message of type ${messageDetails.messageType} 
                to participant ${messageDetails.participantId} 
                in study ${messageDetails.studyId}
                """.trimIndent(), e)
            return MessageOutcome(
                messageDetails.messageType,
                OffsetDateTime.now(),
                messageDetails.participantId,
                messageDetails.url,
                false,
                "message not sent",
                messageDetails.studyId
            )
        }
    }

    override fun sendMessages(organizationId: UUID, messageDetailsList: List<MessageDetails>): List<MessageOutcome> {
        return messageDetailsList.map { messageDetails -> sendMessage(organizationId, messageDetails) }
    }
}