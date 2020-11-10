package com.openlattice.chronicle.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;
import java.util.UUID;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 *
 * POJO for the core feature datasets of chronicle
 */
public class ChronicleCoreAppConfig {

    private final UUID hasEntitySetId;
    private final UUID participatedInEntitySetId;
    private final UUID metadataEntitySetId;
    private final UUID partOfEntitySetId;
    private final UUID notificationEntitySetId;
    private       UUID participantEntitySetId;
    private final UUID studiesEntitySetId;

    @JsonCreator
    public ChronicleCoreAppConfig(
            @JsonProperty( SerializationConstants.ENTITY_SET_ID ) UUID hasEntitySetId,
            @JsonProperty( SerializationConstants.ENTITY_SET_ID ) Optional<UUID> participantEntitySetId,
            @JsonProperty( SerializationConstants.ENTITY_SET_ID ) UUID participatedInEntitySetId,
            @JsonProperty( SerializationConstants.ENTITY_SET_ID ) UUID metadataEntitySetId,
            @JsonProperty( SerializationConstants.ENTITY_SET_ID ) UUID partOfEntitySetId,
            @JsonProperty( SerializationConstants.ENTITY_SET_ID ) UUID notificationEntitySetId,
            @JsonProperty( SerializationConstants.ENTITY_SET_ID ) UUID studiesEntitySetId
    ) {
        participantEntitySetId.ifPresent( uuid -> this.participantEntitySetId = uuid );

        this.hasEntitySetId = hasEntitySetId;
        this.participatedInEntitySetId = participatedInEntitySetId;
        this.metadataEntitySetId = metadataEntitySetId;
        this.partOfEntitySetId = partOfEntitySetId;
        this.notificationEntitySetId = notificationEntitySetId;
        this.studiesEntitySetId = studiesEntitySetId;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_ID )
    public UUID getHasEntitySetId() {
        return hasEntitySetId;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_ID )
    public UUID getParticipatedInEntitySetId() {
        return participatedInEntitySetId;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_ID )
    public UUID getMetadataEntitySetId() {
        return metadataEntitySetId;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_ID )
    public UUID getPartOfEntitySetId() {
        return partOfEntitySetId;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_ID )
    public UUID getNotificationEntitySetId() {
        return notificationEntitySetId;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_ID )
    public UUID getParticipantEntitySetId() {
        return participantEntitySetId;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_ID )
    public UUID getStudiesEntitySetId() {
        return studiesEntitySetId;
    }
}
