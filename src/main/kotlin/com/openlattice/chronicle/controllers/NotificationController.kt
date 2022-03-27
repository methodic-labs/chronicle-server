package com.openlattice.chronicle.controllers

import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.AuthorizingComponent
import com.openlattice.chronicle.base.OK
import com.openlattice.chronicle.base.OK.Companion.ok
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.notifications.DeliveryType
import com.openlattice.chronicle.notifications.NotificationApi
import com.openlattice.chronicle.notifications.NotificationApi.Companion.CONTROLLER
import com.openlattice.chronicle.notifications.NotificationApi.Companion.STUDY_ID_PATH
import com.openlattice.chronicle.notifications.NotificationApi.Companion.STATUS_PATH
import com.openlattice.chronicle.notifications.NotificationApi.Companion.STUDY_ID
import com.openlattice.chronicle.notifications.NotificationType
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
    override val authorizationManager: AuthorizationManager,
) : NotificationApi, AuthorizingComponent {
    override fun getResearcherPhoneNumber(principalId: String): String {
        TODO("Not yet implemented")
    }

    override fun setResearcherPhoneNumber(principalId: String, phoneNumber: String) {
        TODO("Not yet implemented")
    }

    override fun verifyResearcherPhoneNumber(phoneNumber: String, confirmationCode: String) {
        TODO("Not yet implemented")
    }

    override fun getResearcherNotificationSettings(
        studyId: UUID,
        principalId: String,
    ): Map<NotificationType, Set<DeliveryType>> {
        TODO("Not yet implemented")
    }

    override fun setResearcherNotificationSettings(
        studyId: UUID,
        principalId: String,
        settings: Map<NotificationType, Set<DeliveryType>>,
    ): OK {
        TODO("Not yet implemented")
    }

    override fun setResearcherNotificationSettings(
        studyId: UUID,
        principalId: String,
        notificationType: NotificationType,
        deliveryTypes: Set<DeliveryType>,
    ): OK {
        TODO("Not yet implemented")
    }

    override fun getResearcherNotificationSetting(
        studyId: UUID,
        principalId: String,
        notificationType: NotificationType,
    ): Set<DeliveryType> {
        TODO("Not yet implemented")
    }

    @RequestMapping(
        path = [STUDY_ID_PATH],
        method = [RequestMethod.POST]
    )
    override fun sendNotifications(
        @PathVariable(value = STUDY_ID) studyId: UUID,
        @RequestBody participantNotificationList: List<ParticipantNotification>,
    ): OK {
        ensureWriteAccess(AclKey(studyId))
        check(studyService.isValidStudy(studyId)) { "Invalid study id specified." }
        //Make sure the calling user has permission to send notifications to all the acl keys.
        notificationService.sendNotifications(studyId, participantNotificationList)
        return ok
    }

    @RequestMapping(
        path = [STATUS_PATH],
        method = [RequestMethod.POST]
    )
    @ResponseStatus(HttpStatus.OK)
    override fun updateNotificationStatus(
        @RequestParam(value = "MessageSid") messageId: String,
        @RequestParam(value = "MessageStatus") messageStatus: String,
    ): OK {
        notificationService.updateNotificationStatus(messageId, messageStatus)
        return ok

    }

}
