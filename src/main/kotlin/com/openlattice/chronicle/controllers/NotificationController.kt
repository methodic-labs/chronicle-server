package com.openlattice.chronicle.controllers

import com.openlattice.chronicle.auditing.AuditEventType
import com.openlattice.chronicle.auditing.AuditableEvent
import com.openlattice.chronicle.auditing.AuditedOperationBuilder
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.AuthorizingComponent
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.notifications.Notification
import com.openlattice.chronicle.notifications.NotificationApi
import com.openlattice.chronicle.notifications.NotificationApi.Companion.CONTROLLER
import com.openlattice.chronicle.notifications.NotificationApi.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.notifications.NotificationApi.Companion.ORGANIZATION_ID_PATH
import com.openlattice.chronicle.notifications.NotificationApi.Companion.STATUS_PATH
import com.openlattice.chronicle.notifications.NotificationDetails
import com.openlattice.chronicle.notifications.NotificationStatus
import com.openlattice.chronicle.services.notifications.NotificationService
import com.openlattice.chronicle.storage.StorageResolver
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.*
import java.time.OffsetDateTime
import java.util.*
import javax.inject.Inject


/**
 * @author Todd Bergman <todd@openlattice.com>
 */

@RestController
@RequestMapping(CONTROLLER)
class NotificationController @Inject constructor(
    val storageResolver: StorageResolver,
    val idGenerationService: HazelcastIdGenerationService,
    val notificationService: NotificationService,
    override val auditingManager: AuditingManager,
    override val authorizationManager: AuthorizationManager
) : NotificationApi, AuthorizingComponent {

    @RequestMapping(
        path = [ORGANIZATION_ID_PATH],
        method = [RequestMethod.POST]
    )
    override fun sendNotifications(
        @PathVariable(value = ORGANIZATION_ID) organizationId :UUID,
        @RequestBody notificationDetailsList: List<NotificationDetails>
    ) {
        ensureAuthenticated()
        notificationService.sendNotifications(organizationId, notificationDetailsList)
    }

    @RequestMapping(path = [STATUS_PATH],
        method = [RequestMethod.POST])
    @ResponseStatus(HttpStatus.OK)
    override fun updateNotificationStatus(
        @RequestParam(value = "MessageSid") messageId: String,
        @RequestParam(value = "MessageStatus") messageStatus: String
    ) {
        notificationService.updateNotificationStatus(messageId, messageStatus)
    }

}
