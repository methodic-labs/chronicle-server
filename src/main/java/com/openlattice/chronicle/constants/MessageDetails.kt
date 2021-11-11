package com.openlattice.chronicle.constants

import java.time.OffsetDateTime
import java.util.*

class MessageDetails(
    val messageType: String,
    val dateTime: OffsetDateTime,
    val participantId: String,
    val phoneNumber: String,
    val url: String
) {
    override fun hashCode(): Int {
        return Objects
            .hash(
                messageType,
                dateTime,
                participantId,
                phoneNumber
            )
    }
}