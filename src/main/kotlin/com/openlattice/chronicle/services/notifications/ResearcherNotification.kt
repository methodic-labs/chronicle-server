package com.openlattice.chronicle.services.notifications

import com.openlattice.chronicle.notifications.DeliveryType
import com.openlattice.chronicle.notifications.NotificationType
import java.time.OffsetDateTime
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class ResearcherNotification(
    val emails: Set<String>,
    val phoneNumbers: Set<String>,
    val notificationType: NotificationType,
    val deliveryType: EnumSet<DeliveryType>,
    val message: String,
    val dateTime: OffsetDateTime = OffsetDateTime.now(),
)