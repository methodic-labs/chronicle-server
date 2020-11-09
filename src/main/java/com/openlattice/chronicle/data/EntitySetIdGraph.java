package com.openlattice.chronicle.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 *
 * POJO to represent a source, edge and destination entity set id triplet
 */
public class EntitySetIdGraph {
    private final UUID srcEntitySetId;
    private final UUID edgeEntitySetId;
    private final UUID dstEntitySetId;

    @JsonCreator
    public EntitySetIdGraph(
            @JsonProperty (SerializationConstants.SRC) UUID srcEntitySetId,
            @JsonProperty (SerializationConstants.EDGE) UUID edgeEntitySetId,
            @JsonProperty (SerializationConstants.DST) UUID dstEntitySetId
    ) {
        this.dstEntitySetId = dstEntitySetId;
        this.edgeEntitySetId = edgeEntitySetId;
        this.srcEntitySetId = srcEntitySetId;
    }

    @JsonProperty (SerializationConstants.SRC)
    public UUID getSrcEntitySetId() {
        return srcEntitySetId;
    }

    @JsonProperty (SerializationConstants.EDGE)
    public UUID getEdgeEntitySetId() {
        return edgeEntitySetId;
    }

    @JsonProperty (SerializationConstants.DST)
    public UUID getDstEntitySetId() {
        return dstEntitySetId;
    }
}
