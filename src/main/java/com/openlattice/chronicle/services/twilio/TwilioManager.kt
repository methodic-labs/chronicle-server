package com.openlattice.chronicle.services.twilio

import com.openlattice.chronicle.data.MessageDetails
import com.openlattice.chronicle.data.MessageOutcome
import java.util.*

/**
 * @author toddbergman &lt;todd@openlattice.com&gt;
 */
interface TwilioManager {
    fun sendMessages(organizationId: UUID, messageDetailsList: List<MessageDetails>): List<MessageOutcome>
}