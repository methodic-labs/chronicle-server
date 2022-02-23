package com.openlattice.chronicle.services.twilio

import com.openlattice.chronicle.notifications.Notification

/**
 * @author Todd Bergman <todd@openlattice.com>
 */

interface TwilioManager {
    fun sendNotifications(notifications: List<Notification>): List<Notification>
}