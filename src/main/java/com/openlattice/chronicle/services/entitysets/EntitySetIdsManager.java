package com.openlattice.chronicle.services.entitysets;

import com.openlattice.chronicle.constants.*;

import java.util.Map;
import java.util.UUID;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public interface EntitySetIdsManager {
    Map<AppComponent, Map<UUID, Map<CollectionTemplateTypeName, UUID>>> getEntitySetIdsByOrgId();

    UUID getParticipantEntitySetId( UUID organizationId, UUID studyId );

    // entity set id in legacy context
    UUID getLegacyEntitySetId( String entitySetName );

    // entity set id in either app configs context / legacy
    UUID getEntitySetId(
            UUID organizationId,
            AppComponent appComponent,
            CollectionTemplateTypeName templateName,
            String entitySetName
    );

    // get entity set id from a app configs context
    UUID getEntitySetId(
            UUID organizationId,
            AppComponent appComponent,
            CollectionTemplateTypeName templateTypeName
    );
}
