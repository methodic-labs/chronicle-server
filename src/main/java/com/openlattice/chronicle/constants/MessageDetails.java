package com.openlattice.chronicle.data;

import java.time.OffsetDateTime;
import java.util.Objects;

public class MessageDetails {

    private String          messageType;
    private OffsetDateTime  dateTime;
    private String          participantId;
    private String          phoneNumber;
    private String          url;

    public MessageDetails(
            String messageType,
            OffsetDateTime dateTime,
            String participantId,
            String phoneNumber,
            String url) {
        this.messageType = messageType;
        this.dateTime = dateTime;
        this.participantId = participantId;
        this.phoneNumber = phoneNumber;
        this.url = url;
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

    public String getPhoneNumber() { return phoneNumber; }

    public String getUrl() { return url; };

    @Override public int hashCode() {
        return Objects
                .hash(
                        messageType,
                        dateTime,
                        participantId,
                        phoneNumber
                );
    }
}

