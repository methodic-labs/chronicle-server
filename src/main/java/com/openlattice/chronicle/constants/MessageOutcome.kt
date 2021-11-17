package com.openlattice.chronicle.constants

import com.openlattice.chronicle.data.MessageType
import java.time.OffsetDateTime
import java.util.*

data class MessageOutcome(
    val messageType: MessageType,
    val dateTime: OffsetDateTime,
    val participantId: String,
    val url: String,
    var isSuccess: Boolean,
    val sid: String,
    val studyId :UUID,
)