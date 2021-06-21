package com.openlattice.chronicle.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;

import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 *
 * POJO for data collection component of chronicle and its associated entity set ids
 */
public class ChronicleDataCollectionAppConfig {
    private final UUID recordedByEntitySetId;
    private final UUID deviceEntitySetId;
    private final UUID usedByEntitySetId;
    private final UUID userAppsEntitySetId;
    private final UUID preprocessedDataEntitySetId;
    private final UUID appDataEntitySetId;
    private final UUID appsDictionaryEntitySetId;

    @JsonCreator
    public ChronicleDataCollectionAppConfig(
            @JsonProperty( SerializationConstants.ENTITY_SET_ID ) UUID appsDictionaryEntitySetId,
            @JsonProperty( SerializationConstants.ENTITY_SET_ID ) UUID recordedByEntitySetId,
            @JsonProperty( SerializationConstants.ENTITY_SET_ID ) UUID deviceEntitySetId,
            @JsonProperty( SerializationConstants.ENTITY_SET_ID ) UUID usedByEntitySetId,
            @JsonProperty( SerializationConstants.ENTITY_SET_ID ) UUID userAppsEntitySetId,
            @JsonProperty( SerializationConstants.ENTITY_SET_ID ) UUID preprocessedDataEntitySetId,
            @JsonProperty( SerializationConstants.ENTITY_SET_ID ) UUID appDataEntitySetId
    ) {
        this.appsDictionaryEntitySetId = appsDictionaryEntitySetId;
        this.recordedByEntitySetId = recordedByEntitySetId;
        this.deviceEntitySetId = deviceEntitySetId;
        this.usedByEntitySetId = usedByEntitySetId;
        this.userAppsEntitySetId = userAppsEntitySetId;
        this.preprocessedDataEntitySetId = preprocessedDataEntitySetId;
        this.appDataEntitySetId = appDataEntitySetId;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_ID )
    public UUID getRecordedByEntitySetId() {
        return recordedByEntitySetId;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_ID )
    public UUID getDeviceEntitySetId() {
        return deviceEntitySetId;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_ID )
    public UUID getUsedByEntitySetId() {
        return usedByEntitySetId;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_ID )
    public UUID getUserAppsEntitySetId() {
        return userAppsEntitySetId;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_ID )
    public UUID getPreprocessedDataEntitySetId() {
        return preprocessedDataEntitySetId;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_ID )
    public UUID getAppDataEntitySetId() {
        return appDataEntitySetId;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_ID )
    public UUID getAppsDictionaryEntitySetId() {
        return appsDictionaryEntitySetId;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_IDS )
    public Set<UUID> getAllEntitySetIds() {
        return Sets.newHashSet(
                recordedByEntitySetId,
                deviceEntitySetId,
                usedByEntitySetId,
                userAppsEntitySetId,
                preprocessedDataEntitySetId,
                appDataEntitySetId,
                appsDictionaryEntitySetId
        ).stream().filter( Objects::nonNull ).collect( Collectors.toSet() );
    }
}
