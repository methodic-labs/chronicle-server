package com.openlattice.chronicle.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.data.EntityKey;
import org.apache.commons.lang3.tuple.Triple;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public class EntitiesAndEdges {

    private final  Map<EntityKey, Map<UUID, Set<Object>>>       entityByEntityKey;
    private final  Set<Triple<EntityKey, EntityKey, EntityKey>> srcEdgeDstEntityKeys;

    @JsonCreator
    public EntitiesAndEdges(
            Map<EntityKey, Map<UUID, Set<Object>>> entityByEntityKey,
            Set<Triple<EntityKey, EntityKey, EntityKey>> srcEdgeDstEntityKeys ) {
        this.entityByEntityKey = entityByEntityKey;
        this.srcEdgeDstEntityKeys = srcEdgeDstEntityKeys;
    }

    @JsonProperty( SerializationConstants.ENTITY_BY_ENTITY_KEY )
    public Map<EntityKey, Map<UUID, Set<Object>>> getEntityByEntityKey() {
        return entityByEntityKey;
    }

    @JsonProperty( SerializationConstants.EDGE_ENTITY_KEYS )
    public Set<Triple<EntityKey, EntityKey, EntityKey>> getSrcEdgeDstEntityKeys() {
        return srcEdgeDstEntityKeys;
    }
}
