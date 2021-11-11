package com.openlattice.chronicle.constants

import java.time.OffsetDateTime
import java.util.*

class MessageOutcome(
    val messageType: String,
    val dateTime: OffsetDateTime,
    val participantId: String,
    val url: String,
    var isSuccess: Boolean,
    val sid: String
) {
    override fun hashCode(): Int {
        return Objects
            .hash(
                messageType,
                dateTime,
                participantId,
                isSuccess,
                sid
            )
    }
}