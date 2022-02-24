package com.openlattice.chronicle.services.notifications

import com.openlattice.chronicle.notifications.Notification
import java.sql.Connection

/**
 * @author Todd Bergman <todd@openlattice.com>
 */

interface NotificationManager {
    fun sendNotifications(connection: Connection, notifications :List<Notification>)
    fun updateNotificationStatus(messageSid: String, status :String)
}
