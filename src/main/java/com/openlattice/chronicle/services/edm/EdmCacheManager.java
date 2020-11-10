package com.openlattice.chronicle.services.edm;

import com.openlattice.edm.type.PropertyType;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public interface EdmCacheManager {
    Map<String, UUID> getLegacyPropertyTypeIds( Set<String> propertyTypeFqns );

    Map<FullQualifiedName, UUID> getPropertyTypeIds( Set<FullQualifiedName> propertyTypeFqns );

    UUID getPropertyTypeId( FullQualifiedName fqn );

    PropertyType getPropertyType( UUID propertyTypeId );

    PropertyType getPropertyType( FullQualifiedName fqn );

}
