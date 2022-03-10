package com.openlattice.chronicle.services.notifications

import com.openlattice.chronicle.auditing.*
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.SecurableObjectType
import com.openlattice.chronicle.authorization.principals.Principals
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.notifications.Notification
import com.openlattice.chronicle.notifications.NotificationDetails
import com.openlattice.chronicle.notifications.NotificationStatus
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.services.twilio.TwilioService
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.NOTIFICATIONS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.MESSAGE_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.NOTIFICATION_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STATUS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.UPDATED_AT
import com.openlattice.chronicle.storage.StorageResolver
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.sql.Connection
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.ExecutionException


/**
 * @author Todd Bergman <todd@openlattice.com>
 */

@Service
class NotificationService(
    private val storageResolver: StorageResolver,
    private val authorizationService: AuthorizationManager,
    val idGenerationService: HazelcastIdGenerationService,
    private val twilioService :TwilioService,
    override val auditingManager: AuditingManager,
) : NotificationManager, AuditingComponent {

    companion object {
        private val logger = LoggerFactory.getLogger(NotificationService::class.java)

        private val NOTIFICAITON_COLUMNS = NOTIFICATIONS.columns.joinToString(",") { it.name }

        private val UPDATE_NOTIFICATION_COLUMNS = listOf(
            UPDATED_AT,
            STATUS,
        ).joinToString(",") { it.name }

        private val INSERT_NOTIFICATION_SQL = """
            INSERT INTO ${NOTIFICATIONS.name} (${NOTIFICAITON_COLUMNS}) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """.trimIndent()

        private val GET_NOTIFICATION_ID_FROM_MESSAGE_ID_SQL = "SELECT * FROM ${NOTIFICATIONS.name} WHERE ${MESSAGE_ID.name} = ?"

        private val UPDATE_NOTIFICATION_SQL = """
            UPDATE ${NOTIFICATIONS.name}
            SET (${UPDATE_NOTIFICATION_COLUMNS}) = (?,?)
            WHERE ${NOTIFICATION_ID.name} = ?
        """.trimIndent()
    }

    private fun insertNotifications(connection: Connection, notifications: List<Notification>): Int {
        val ps = connection.prepareStatement(INSERT_NOTIFICATION_SQL)
        notifications.forEach { notification ->
            ps.setObject(1, notification.id)
            ps.setObject(2, notification.candidateId)
            ps.setObject(3, notification.organizationId)
            ps.setObject(4, notification.studyId)
            ps.setObject(5, notification.createdAt)
            ps.setObject(6, notification.updatedAt)
            ps.setObject(7, notification.messageId)
            ps.setObject(8, notification.type)
            ps.setObject(9, notification.status)
            ps.setObject(10, notification.body)
            ps.setObject(11, notification.email)
            ps.setObject(12, notification.phone)
            ps.addBatch()
            authorizationService.createUnnamedSecurableObject(
                connection = connection,
                aclKey = AclKey(notification.id),
                principal = Principals.getCurrentUser(),
                objectType = SecurableObjectType.Notification
            )
        }
        return ps.executeBatch().sum()
    }

    private fun getNotificationByMessageId(messageId: String) :Notification? {
        val hds = storageResolver.getPlatformStorage()
        val notification = hds.connection.use { connection ->
            connection.prepareStatement(GET_NOTIFICATION_ID_FROM_MESSAGE_ID_SQL).use { ps ->
                ps.setObject(1, messageId)
                ps.executeQuery().use { rs ->
                    check(rs.next()) { "No row returned for messageId=$messageId" }
                    ResultSetAdapters.notification(rs)
                }
            }
        }
        return notification;
    }

    private fun updateNotification(connection :Connection, notification: Notification) {
        connection.prepareStatement(UPDATE_NOTIFICATION_SQL).use { ps ->
            ps.setObject(1, notification.updatedAt)
            ps.setObject(2, notification.status)
            ps.setObject(3, notification.id)
            ps.executeUpdate()
        }
    }

    override fun updateNotificationStatus(messageId: String, status: String) {
        try {
            val notification :Notification? = getNotificationByMessageId(messageId);
            val shouldUpdateStatus :Boolean = status == NotificationStatus.failed.name
                    || status == NotificationStatus.undelivered.name
                    || status == NotificationStatus.delivery_unknown.name
                    || status == NotificationStatus.delivered.name
                    || status == NotificationStatus.sent.name
            if (notification != null && shouldUpdateStatus) {
                notification.status = status
                notification.updatedAt = OffsetDateTime.now()
                val hds = storageResolver.getPlatformStorage()
                hds.connection.use { connection-> updateNotification(connection, notification) }
            }
            logger.info("Message status updated to $status for notification with SID $messageId")
        } catch (e: ExecutionException) {
            logger.error("Unable to update message status for notification with SID $messageId", e)
        }

    }

    override fun sendNotifications(organizationId: UUID, notificationDetailsList: List<NotificationDetails>) {
        val hds = storageResolver.getPlatformStorage()
        val notificationAuditEvents = mutableListOf<AuditableEvent>();
        val notifications :List<Notification> = notificationDetailsList.map { notificationDetails ->
            val messageText = "Chronicle device enrollment:  Please download app from your app store and click on ${notificationDetails.url} to enroll your device."
            val notificationId = idGenerationService.getNextId();
            notificationAuditEvents.add(
                AuditableEvent(
                    AclKey(notificationId),
                    eventType = AuditEventType.SEND_SMS_NOTIFICATION,
                    description = "send message to ${notificationDetails.candidateId}",
                    study = notificationDetails.studyId,
                    organization = organizationId,
                )
            )
            Notification(
                notificationId,
                notificationDetails.candidateId,
                organizationId,
                notificationDetails.studyId,
                OffsetDateTime.now(),
                OffsetDateTime.now(),
                NotificationStatus.sent.name,
                "notification not sent for id, $notificationId",
                notificationDetails.notificationType,
                messageText,
                null,
                notificationDetails.phoneNumber
            )
        }
        logger.info("preparing to send batch of ${notifications.size} messages to participants")
        hds.connection.use { connection ->
            AuditedOperationBuilder<Unit>(connection, auditingManager)
                .operation { conn ->
                    val notificationOutcomes = twilioService.sendNotifications(notifications)
                    insertNotifications(conn, notificationOutcomes)
                }
                .audit { notificationAuditEvents }
                .buildAndRun()
        }
    }
}
