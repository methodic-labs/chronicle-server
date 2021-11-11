package com.openlattice.chronicle.services.twilio;

import com.openlattice.chronicle.constants.MessageOutcome;
import com.openlattice.chronicle.data.MessageDetails;

public interface TwilioManager {

    MessageOutcome sendMessage( String participantId, MessageDetails messageDetails);

}
