package com.openlattice.chronicle.services.twilio

import com.openlattice.chronicle.services.notifications.Notification
import com.twilio.type.PhoneNumber
import java.util.*

/**
 * @author Todd Bergman <todd@openlattice.com>
 */

interface TwilioManager {
    fun sendNotifications(notifications: List<Notification>): List<Notification>
    fun getStudyPhoneNumber(studyId: UUID): PhoneNumber
}
