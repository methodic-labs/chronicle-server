package com.openlattice.chronicle.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.Set;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public class EntityNeighborProperties {
    private Set<FullQualifiedName> entityFqns;
    private Set<FullQualifiedName> edgeFqns;

    @JsonCreator
    public EntityNeighborProperties() {
    }

    @JsonCreator
    public EntityNeighborProperties( Set<FullQualifiedName> entityFqns, Set<FullQualifiedName> edgeFqns ) {
        this.entityFqns = entityFqns;
        this.edgeFqns = edgeFqns;
    }

    @JsonProperty( SerializationConstants.ENTITY_FQNS )
    public Set<FullQualifiedName> getEntityFqns() {
        return entityFqns;
    }

    @JsonProperty( SerializationConstants.EDGE_FQNS )
    public Set<FullQualifiedName> getEdgeFqns() {
        return edgeFqns;
    }
}
