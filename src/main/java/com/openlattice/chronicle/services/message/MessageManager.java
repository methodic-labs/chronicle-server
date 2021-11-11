package com.openlattice.chronicle.services.message;

import java.util.Map;
import java.util.UUID;
/**
 * @author toddbergman &lt;todd@openlattice.com&gt;
 */
public interface MessageManager {

    void sendMessage( UUID organizationId, UUID studyId, String participantId, Map<String, String> messageDetails );
}
