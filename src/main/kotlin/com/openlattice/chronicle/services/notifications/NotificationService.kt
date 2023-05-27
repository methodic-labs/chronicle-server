package com.openlattice.chronicle.services.notifications

import com.openlattice.chronicle.auditing.*
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.Principal
import com.openlattice.chronicle.authorization.SecurableObjectType
import com.openlattice.chronicle.authorization.principals.Principals
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.ids.IdConstants
import com.openlattice.chronicle.notifications.DeliveryType
import com.openlattice.chronicle.notifications.ParticipantNotification
import com.openlattice.chronicle.notifications.NotificationStatus
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.services.candidates.CandidateService
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.services.jobs.ChronicleJob
import com.openlattice.chronicle.services.jobs.JobService
import com.openlattice.chronicle.services.studies.StudyService
import com.openlattice.chronicle.services.twilio.TwilioService
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.NOTIFICATIONS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.MESSAGE_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.NOTIFICATION_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STATUS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.UPDATED_AT
import com.openlattice.chronicle.storage.StorageResolver
import jodd.mail.Email
import jodd.mail.RFC2822AddressParser
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.sql.Connection
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.ExecutionException


/**
 * @author Matthew Tamayo-Rios <matthew@getmethodic.com>
 * @author Todd Bergman <todd@openlattice.com>
 */

@Service
class NotificationService(
    private val storageResolver: StorageResolver,
    private val authorizationService: AuthorizationManager,
    private val enrollmentService: EnrollmentManager,
    private val candidateService: CandidateService,
    private val studyService: StudyService,
    private val jobService: JobService,
    private val idGenerationService: HazelcastIdGenerationService,
    private val twilioService: TwilioService,
    override val auditingManager: AuditingManager,
) : NotificationManager, AuditingComponent {

    companion object {
        private val logger = LoggerFactory.getLogger(NotificationService::class.java)

        const val INITIAL_STATUS = "queued"
        private val NOTIFICATION_COLUMNS = NOTIFICATIONS.columns.joinToString(",") { it.name }

        private val UPDATE_NOTIFICATION_COLUMNS = listOf(
            UPDATED_AT,
            STATUS,
        ).joinToString(",") { it.name }

        private val INSERT_NOTIFICATION_SQL = """
            INSERT INTO ${NOTIFICATIONS.name} (${NOTIFICATION_COLUMNS}) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """.trimIndent()

        private val GET_NOTIFICATION_ID_FROM_MESSAGE_ID_SQL =
            "SELECT * FROM ${NOTIFICATIONS.name} WHERE ${MESSAGE_ID.name} = ?"

        private val UPDATE_NOTIFICATION_SQL = """
            UPDATE ${NOTIFICATIONS.name}
            SET (${UPDATE_NOTIFICATION_COLUMNS}) = (?,?)
            WHERE ${NOTIFICATION_ID.name} = ?
        """.trimIndent()

        //Safety check in case twilio adds a matching status
        init {
            NotificationStatus.values().none { it.name == INITIAL_STATUS }
        }
    }

    private fun insertNotifications(
        connection: Connection,
        notifications: List<Notification>,
        principal: Principal = Principals.getCurrentUser(),
    ): Int {
        val ps = connection.prepareStatement(INSERT_NOTIFICATION_SQL)
        notifications.forEach { notification ->
            ps.setObject(1, notification.id)
            ps.setObject(2, notification.studyId)
            ps.setObject(3, notification.participantId)
            ps.setObject(4, notification.createdAt)
            ps.setObject(5, notification.updatedAt)
            ps.setString(6, notification.messageId)
            ps.setString(7, notification.status)
            ps.setString(8, notification.notificationType.name)
            ps.setString(9, notification.deliveryType.name)
            ps.setString(10, notification.subject)
            ps.setString(11, notification.body)
            ps.setString(12, notification.destination)
            ps.setBoolean(13, notification.html)
            ps.addBatch()
            authorizationService.createUnnamedSecurableObject(
                connection = connection,
                aclKey = AclKey(notification.id),
                principal = principal,
                objectType = SecurableObjectType.Notification
            )
        }
        return ps.executeBatch().sum()
    }

    private fun getNotificationByMessageId(messageId: String): Notification {
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

        return notification
    }

    private fun updateNotification(connection: Connection, notification: Notification) {
        connection.prepareStatement(UPDATE_NOTIFICATION_SQL).use { ps ->
            ps.setObject(1, notification.updatedAt)
            ps.setObject(2, notification.status)
            ps.setObject(3, notification.id)
            ps.executeUpdate()
        }
    }

    override fun updateNotificationStatus(messageId: String, status: String) {
        try {
            val notification: Notification? = getNotificationByMessageId(messageId);
            val shouldUpdateStatus: Boolean = status == NotificationStatus.failed.name
                    || status == NotificationStatus.undelivered.name
                    || status == NotificationStatus.delivery_unknown.name
                    || status == NotificationStatus.delivered.name
                    || status == NotificationStatus.sent.name
            if (notification != null && shouldUpdateStatus) {
                notification.status = status
                notification.updatedAt = OffsetDateTime.now()
                val hds = storageResolver.getPlatformStorage()
                hds.connection.use { connection -> updateNotification(connection, notification) }
            }
            logger.info("Message status updated to $status for notification with SID $messageId")
        } catch (e: ExecutionException) {
            logger.error("Unable to update message status for notification with SID $messageId", e)
        }

    }

    override fun sendResearcherNotifications(
        connection: Connection,
        studyId: UUID,
        researcherNotifications: List<ResearcherNotification>,
        html: Boolean,
        principal: Principal,
    ): Int {
        val notifications: List<Notification> =
            researcherNotifications.asSequence().map { researcherNotification ->
                //val messageText = "Chronicle device enrollment:  Please download app from your app store and click on ${notificationDetails.url} to enroll your device."
                //This ensure that we only send e-mails to valid e-mail addresses.
                researcherNotification.emails.filter { it.isNotBlank() && RFC2822AddressParser.LOOSE.parse(it).isValid }
                    .map { email ->
                        val notificationId = idGenerationService.getNextId()
                        Notification(
                            notificationId,
                            studyId,
                            "",
                            status = INITIAL_STATUS,
                            messageId = notificationId.toString(),
                            notificationType = researcherNotification.notificationType,
                            deliveryType = DeliveryType.EMAIL,
                            subject = researcherNotification.subject,
                            body = researcherNotification.message,
                            destination = email,
                            html = html
                        )
                    } + researcherNotification.phoneNumbers.filter { it.isNotBlank() }.map { phoneNumber ->
                    val notificationId = idGenerationService.getNextId()
                    Notification(
                        notificationId,
                        studyId,
                        "",
                        status = INITIAL_STATUS,
                        messageId = notificationId.toString(),
                        notificationType = researcherNotification.notificationType,
                        deliveryType = DeliveryType.SMS,
                        body = researcherNotification.message,
                        destination = phoneNumber
                    )
                }
            }.flatten().toList()
        logger.info("Queueing batch of ${notifications.size} of notifications")
        insertNotifications(connection, notifications, principal)
        notifications.forEach {
            jobService.createJob(
                connection,
                ChronicleJob(
                    id = idGenerationService.getNextId()
                            definition = it,
                    securablePrincipalId = IdConstants.METHODIC.id,
                    principal = principal
                )
            )
        }
        return notifications.size
    }

    override fun sendNotifications(
        connection: Connection,
        studyId: UUID,
        participantNotifications: List<ParticipantNotification>,
    ): Int {
        val notifications: List<Notification> =
            participantNotifications.asSequence().mapNotNull { participantNotification ->
                //val messageText = "Chronicle device enrollment:  Please download app from your app store and click on ${notificationDetails.url} to enroll your device."
                val participant = enrollmentService.getParticipant(studyId, participantNotification.participantId)
                val phoneNumber = participant.candidate.phoneNumber ?: return@mapNotNull null
                participantNotification.deliveryType.map { deliveryType ->
                    val notificationId = idGenerationService.getNextId()
                    Notification(
                        notificationId,
                        studyId,
                        participantNotification.participantId,
                        status = INITIAL_STATUS,
                        messageId = "",
                        notificationType = participantNotification.notificationType,
                        deliveryType = deliveryType,
                        body = participantNotification.message,
                        destination = when (deliveryType) {
                            DeliveryType.SMS -> phoneNumber
                            DeliveryType.EMAIL -> checkNotNull(participant.candidate.email) { "Email cannot be null for email delivery type." }
                        }
                    )
                }
            }.flatten().toList()
        logger.info("preparing to send batch of ${notifications.size} messages to participants")
        insertNotifications(connection, notifications)
        notifications.forEach {
            jobService.createJob(connection, ChronicleJob(id = idGenerationService.getNextId(), definition = it))
        }
        return notifications.size
    }

    override fun sendNotifications(studyId: UUID, participantNotifications: List<ParticipantNotification>) {
        storageResolver.getPlatformStorage().connection.use { connection ->
            AuditedTransactionBuilder<Unit>(connection, auditingManager)
                .transaction { conn ->
//                    val notificationOutcomes = twilioService.sendNotifications(notifications)
                    sendNotifications(conn, studyId, participantNotifications)
                }
                .audit {
                    listOf(
                        AuditableEvent(
                            AclKey(studyId),
                            eventType = AuditEventType.QUEUE_NOTIFICATIONS,
                            description = "Queued $it notifications.",
                            study = studyId,
                        )
                    )
                }
                .buildAndRun()
        }
    }
}
