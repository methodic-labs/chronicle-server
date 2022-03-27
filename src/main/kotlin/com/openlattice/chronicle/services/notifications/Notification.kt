package com.openlattice.chronicle.services.notifications

import com.openlattice.chronicle.services.jobs.ChronicleJobDefinition
import com.openlattice.chronicle.notifications.DeliveryType
import com.openlattice.chronicle.notifications.NotificationType
import java.time.OffsetDateTime
import java.util.*

/**
 * @author Matthew Tamayo-Rios &lt;matthew@getmethodic.com>&gt;
 *
 * Definition of a notification that has been registered with the platform.
 *
 *     @param deliveryType Determines what will be expected in [destination] field
 *     @param html If [deliveryType] is [DeliveryType.EMAIL] controls type of e-mail being sent, otherwise it is ignored.
 */
data class Notification(
    val id: UUID,
    val studyId: UUID,
    val participantId: String,
    val createdAt: OffsetDateTime = OffsetDateTime.now(),
    var updatedAt: OffsetDateTime = OffsetDateTime.now(),
    var status: String,
    var messageId: String,
    val notificationType: NotificationType,
    val deliveryType: DeliveryType,
    val subject: String = "",
    val body: String,
    val destination: String,
    val html: Boolean = false,
) : ChronicleJobDefinition