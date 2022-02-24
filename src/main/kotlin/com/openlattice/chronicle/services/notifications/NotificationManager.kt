package com.openlattice.chronicle.services.notifications

import com.openlattice.chronicle.notifications.NotificationDetails
import java.util.*

/**
 * @author Todd Bergman <todd@openlattice.com>
 */

interface NotificationManager {
    fun sendNotifications(organizationId : UUID, notificationDetailsList :List<NotificationDetails>)
    fun updateNotificationStatus(messageSid: String, status :String)
}
