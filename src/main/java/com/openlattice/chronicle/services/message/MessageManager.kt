package com.openlattice.chronicle.services.message

import java.util.*

public interface MessageManager {
    fun sendMessage(
        organizationId: UUID,
        studyId: UUID,
        participantId: String,
        messageDetails: Map<String, String>
    )
}