package com.openlattice.chronicle.services.message

import com.openlattice.chronicle.data.MessageDetails
import java.util.*

public interface MessageManager {
    fun sendMessage(
        organizationId: UUID,
        studyId: UUID,
        participantId: String,
        messageDetails: List<MessageDetails>
    )
}