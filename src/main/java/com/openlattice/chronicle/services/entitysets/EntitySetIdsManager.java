package com.openlattice.chronicle.services.entitysets;

import com.openlattice.chronicle.constants.*;
import com.openlattice.chronicle.data.*;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public interface EntitySetIdsManager {
    Map<AppComponent, Map<UUID, Map<CollectionTemplateTypeName, UUID>>> getEntitySetIdsByOrgId();

    // app settings
    Map<String, Object> getOrgAppSettings(AppComponent appComponent, UUID organizationId);

    Map<String, Object> getOrgAppSettings(String appName, UUID organizationId);

    EntitySetsConfig getEntitySetsConfig(UUID organizationId, UUID studyId, Set<AppComponent> components);
}
