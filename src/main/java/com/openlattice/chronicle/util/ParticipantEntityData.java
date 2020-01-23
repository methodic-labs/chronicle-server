package com.openlattice.chronicle.util;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializationConstants;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;

/**
 * @author Alfonce Nzioka &lt;alfonce@openlattice.com&gt;
 */
public class ParticipantEntityData  {
    private final String participantId;
    private final UUID participantEntityKeyId;

    public ParticipantEntityData( String participantId, UUID participantEntityKeyId ) {
        this.participantEntityKeyId = participantEntityKeyId;
        this.participantId = participantId;
    }

    public String getParticipantId() {
        return participantId;
    }

    public UUID getParticipantEntityKeyId() {
        return participantEntityKeyId;
    }
}
