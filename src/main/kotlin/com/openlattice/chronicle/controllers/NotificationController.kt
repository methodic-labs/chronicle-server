package com.openlattice.chronicle.controllers

import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.AuthorizingComponent
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.notifications.NotificationApi
import com.openlattice.chronicle.notifications.NotificationApi.Companion.CONTROLLER
import com.openlattice.chronicle.notifications.NotificationApi.Companion.STUDY_ID_PATH
import com.openlattice.chronicle.notifications.NotificationApi.Companion.STATUS_PATH
import com.openlattice.chronicle.notifications.NotificationApi.Companion.STUDY_ID
import com.openlattice.chronicle.notifications.ParticipantNotification
import com.openlattice.chronicle.services.notifications.NotificationService
import com.openlattice.chronicle.services.studies.StudyService
import com.openlattice.chronicle.storage.StorageResolver
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.*
import java.util.*
import javax.inject.Inject


/**
 * @author Matthew Tamayo-Rios <matthew@openlattice.com>
 * @author Todd Bergman <todd@openlattice.com>
 */

@RestController
@RequestMapping(CONTROLLER)
class NotificationController @Inject constructor(
        val storageResolver: StorageResolver,
        val idGenerationService: HazelcastIdGenerationService,
        val notificationService: NotificationService,
        val studyService: StudyService,
        override val auditingManager: AuditingManager,
        override val authorizationManager: AuthorizationManager
) : NotificationApi, AuthorizingComponent {

    @RequestMapping(
            path = [STUDY_ID_PATH],
            method = [RequestMethod.POST]
    )
    override fun sendNotifications(
            @PathVariable(value = STUDY_ID) studyId: UUID,
            @RequestBody participantNotificationList: List<ParticipantNotification>
    ) {
        ensureWriteAccess(AclKey(studyId))
        check(studyService.isValidStudy(studyId)) { "Invalid study id specified." }
        //Make sure the calling user has permission to send notifications to all the acl keys.
        notificationService.sendNotifications(studyId, participantNotificationList)
    }

    @RequestMapping(
            path = [STATUS_PATH],
            method = [RequestMethod.POST]
    )
    @ResponseStatus(HttpStatus.OK)
    override fun updateNotificationStatus(
            @RequestParam(value = "MessageSid") messageId: String,
            @RequestParam(value = "MessageStatus") messageStatus: String
    ) {
        notificationService.updateNotificationStatus(messageId, messageStatus)
    }

}
