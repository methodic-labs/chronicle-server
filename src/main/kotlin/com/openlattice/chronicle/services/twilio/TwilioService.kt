package com.openlattice.chronicle.services.twilio

import com.openlattice.chronicle.configuration.TwilioConfiguration
import com.openlattice.chronicle.notifications.Notification
import com.openlattice.chronicle.notifications.NotificationApi.Companion.BASE
import com.openlattice.chronicle.notifications.NotificationApi.Companion.STATUS_PATH
import com.openlattice.chronicle.notifications.NotificationStatus
import com.twilio.Twilio
import com.twilio.exception.ApiException
import com.twilio.rest.api.v2010.account.Message
import com.twilio.type.PhoneNumber
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.net.URI

/**
 * @author Todd Bergman <todd@openlattice.com>
 */

@Service
class TwilioService(
    twilioConfiguration: TwilioConfiguration?,
) :TwilioManager {
    companion object {
        protected val logger: Logger = LoggerFactory.getLogger(TwilioService::class.java)
        protected val callbackURL :String = "https://api.openlattice.com$BASE$STATUS_PATH"
        protected val testingCallbackURL :String = "http://da67-75-26-16-62.ngrok.io$BASE$STATUS_PATH"
    }

    init {
        Twilio.init(twilioConfiguration?.sid, twilioConfiguration?.token)
    }

    private val fromPhoneNumber: PhoneNumber = PhoneNumber(twilioConfiguration?.fromPhone)

    private fun sendNotification(notification: Notification) :Notification {
        try {
            val message = Message
                .creator(PhoneNumber(notification.phone), fromPhoneNumber, notification.body)
                .setStatusCallback(URI.create( testingCallbackURL ))
                .create()
            logger.info("message sent to participant ${notification.candidateId} for study ${notification.studyId} in organization ${notification.organizationId}")
            notification.messageId = message.sid
        } catch (e: ApiException) {
            logger.error("Unable to send message of type ${notification.type} to participant ${notification.candidateId} in study ${notification.studyId}".trimIndent(), e)
            notification.status = NotificationStatus.failed.name
        }
        return notification
    }

    override fun sendNotifications(notifications :List<Notification>): List<Notification> {
        return notifications.map { notification -> sendNotification(notification) }
    }
}
