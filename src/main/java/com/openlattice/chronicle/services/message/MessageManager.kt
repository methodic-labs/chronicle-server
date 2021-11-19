package com.openlattice.chronicle.services.message

import com.openlattice.chronicle.data.MessageDetails
import java.util.*

interface MessageManager {
    fun sendMessages(
        organizationId: UUID,
        messageDetails: List<MessageDetails>
    )
}