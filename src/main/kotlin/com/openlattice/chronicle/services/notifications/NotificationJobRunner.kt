package com.openlattice.chronicle.services.notifications

import com.geekbeast.mail.EmailRequest
import com.geekbeast.mail.MailService
import com.openlattice.chronicle.auditing.AuditEventType
import com.openlattice.chronicle.auditing.AuditableEvent
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.notifications.DeliveryType
import com.openlattice.chronicle.services.jobs.AbstractChronicleJobRunner
import com.openlattice.chronicle.services.jobs.ChronicleJob
import com.openlattice.chronicle.services.twilio.TwilioService
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.NOTIFICATIONS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.MESSAGE_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.NOTIFICATION_ID
import java.sql.Connection

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class NotificationJobRunner(
    private val twilioService: TwilioService,
    private val mailService: MailService,
) : AbstractChronicleJobRunner<Notification>() {
    companion object {
        private val UPDATE_NOTIFICATION_MESSAGE_ID_SQL = """
            UPDATE ${NOTIFICATIONS.name} SET ${MESSAGE_ID.name} = ? WHERE ${NOTIFICATION_ID.name} = ?
        """
    }

    override fun accepts(): Class<Notification> = Notification::class.java

    override fun runJob(connection: Connection, job: ChronicleJob): List<AuditableEvent> {
        val notification = job.definition as Notification
        when (notification.deliveryType) {
            DeliveryType.SMS -> updateNotification(connection, twilioService.sendNotification(notification))
            DeliveryType.EMAIL -> mailService.sendEmails(
                listOf(EmailRequest(
                    to = listOf(notification.destination),
                    subject = notification.subject,
                    body = notification.body,
                    html = notification.html
                ))
            )
        }

        return listOf(AuditableEvent(
            AclKey(notification.studyId),
            job.securablePrincipalId,
            job.principal,
            AuditEventType.NOTIFICATION_SENT,
            "Sent ${notification.deliveryType} notification to ${notification.participantId}",
            notification.studyId
        ))
    }

    private fun updateNotification(connection: Connection, notification: Notification) {
        connection.prepareStatement(UPDATE_NOTIFICATION_MESSAGE_ID_SQL).use { ps ->
            ps.setString(1, notification.messageId)
            ps.setObject(2, notification.id)
            ps.executeUpdate()
        }
    }
}