package com.openlattice.chronicle.services.twilio

import com.openlattice.chronicle.constants.MessageDetails
import com.openlattice.chronicle.constants.MessageOutcome

/**
 * @author toddbergman &lt;todd@openlattice.com&gt;
 */
interface TwilioManager {
    fun sendMessage(participantId: String, messageDetails: MessageDetails): MessageOutcome
}