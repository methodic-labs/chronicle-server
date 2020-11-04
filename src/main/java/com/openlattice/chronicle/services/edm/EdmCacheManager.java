package com.openlattice.chronicle.services.edm;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public interface EdmCacheManager {
    Map<String, UUID> getPropertyTypeIds( Set<String> propertyTypeFqns);
}
