package com.openlattice.chronicle.services.twilio;

import com.openlattice.chronicle.constants.MessageOutcome;
import com.openlattice.chronicle.constants.MessageDetails;

/**
 * @author toddbergman &lt;todd@openlattice.com&gt;
 */
public interface TwilioManager {

    MessageOutcome sendMessage( String participantId, MessageDetails messageDetails);

}
