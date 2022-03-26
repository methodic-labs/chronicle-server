package com.openlattice.chronicle.services.notifications

import com.openlattice.chronicle.notifications.ParticipantNotification
import java.util.*

/**
 * @author Todd Bergman <todd@openlattice.com>
 */

interface NotificationManager {
    fun sendNotifications(studyId : UUID, participantNotificationList :List<ParticipantNotification>)
    fun updateNotificationStatus(messageSid: String, status :String)
}
