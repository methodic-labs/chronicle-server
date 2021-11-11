package com.openlattice.chronicle.constants;

import com.openlattice.chronicle.data.MessageType;

import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.UUID;

public class MessageOutcome {

    private String    messageType;
    private OffsetDateTime dateTime;
    private String         participantId;
    private String         url;
    boolean success;
    private String sid;

    public MessageOutcome(
            String messageType,
            OffsetDateTime dateTime,
            String participantId,
            String url,
            boolean success,
            String sid ) {
        this.messageType = messageType;
        this.dateTime = dateTime;
        this.participantId = participantId;
        this.url = url;
        this.success = success;
        this.sid = sid;
    }

    public String getMessageType() {
        return messageType;
    }

    public OffsetDateTime getDateTime() {
        return dateTime;
    }

    public String getParticipantId() {
        return participantId;
    }

    public String getUrl() { return url; };

    public boolean isSuccess() {
        return success;
    }

    public String getSid() {
        return sid;
    }

    @Override public int hashCode() {
        return Objects
                .hash(
                        messageType,
                        dateTime,
                        participantId,
                        success,
                        sid
                );
    }
}
