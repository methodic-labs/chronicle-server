package com.openlattice.chronicle.services;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.openlattice.client.ApiClient;
import com.openlattice.edm.EdmApi;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.entitysets.EntitySetsApi;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import static com.openlattice.chronicle.constants.EdmConstants.ENTITY_SET_NAMES;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public class EdmCacheManager {
    private final Map<UUID, PropertyType>      propertyTypesById    = Maps.newHashMap();
    private final Map<String, UUID>            entitySetIdMap       = Maps.newHashMap();
    private final Map<FullQualifiedName, UUID> propertyTypeIdsByFQN = Maps.newHashMap();

    public EdmCacheManager( ApiCacheManager apiCacheManager ) throws ExecutionException {
        // apiCacheManager
        ApiClient prodApiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
        EdmApi edmApi = prodApiClient.getEdmApi();
        EntitySetsApi entitySetsApi = prodApiClient.getEntitySetsApi();

        // get entity setId map
        entitySetIdMap.putAll( entitySetsApi.getEntitySetIds( ENTITY_SET_NAMES ) );

        // get propertyTypeId map
        Iterable<PropertyType> propertyTypes = edmApi.getPropertyTypes();
        propertyTypesById.putAll(
                StreamSupport
                        .stream( propertyTypes.spliterator(), false )
                        .collect( ImmutableMap.toImmutableMap( PropertyType::getId, Function.identity() ) ) );
        propertyTypeIdsByFQN.putAll( propertyTypesById
                .values()
                .stream()
                .collect( ImmutableMap.toImmutableMap( PropertyType::getType, PropertyType::getId ) ) );
    }

    public UUID getPropertyTypeId( FullQualifiedName fqn ) {
        return this.propertyTypeIdsByFQN.get( fqn );
    }

    public UUID getHistoricalEntitySetId( String entitySetName ) {
        return this.entitySetIdMap.getOrDefault( entitySetName, null );
    }
}
