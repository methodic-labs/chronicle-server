package com.openlattice.chronicle.services.edm;

import com.openlattice.chronicle.constants.*;
import com.openlattice.edm.type.PropertyType;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public interface EdmCacheManager {
    // overloading doesn't work here. javac error: both methods have the same type erasure
    Map<String, UUID> getLegacyPropertyTypeIds( Set<String> propertyTypeFqns );

    Map<FullQualifiedName, UUID> getPropertyTypeIds( Set<FullQualifiedName> propertyTypeFqns );

    UUID getPropertyTypeId( FullQualifiedName fqn );

    PropertyType getPropertyType( UUID propertyTypeId );

    PropertyType getPropertyType( FullQualifiedName fqn );

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
    UUID getEntitySetId (
            UUID organizationId,
            AppComponent appComponent,
            CollectionTemplateTypeName templateTypeName
    );
}
