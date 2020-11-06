package com.openlattice.chronicle.services.edm;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.openlattice.chronicle.services.ApiCacheManager;
import com.openlattice.client.ApiClient;
import com.openlattice.edm.EdmApi;
import com.openlattice.edm.type.PropertyType;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.StreamSupport;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public class EdmCacheService implements EdmCacheManager {
    protected static final Logger logger = LoggerFactory.getLogger( EdmCacheService.class );

    private final Map<UUID, PropertyType>      propertyTypesById    = Maps.newHashMap();
    private final Map<FullQualifiedName, UUID> propertyTypeIdsByFQN = Maps.newHashMap();

    public EdmCacheService( ApiCacheManager apiCacheManager ) throws ExecutionException {

        ApiClient prodApiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
        EdmApi edmApi = prodApiClient.getEdmApi();

        // get propertyTypeIds
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

    @Override
    public UUID getPropertyTypeId( FullQualifiedName fqn ) {
        return this.propertyTypeIdsByFQN.get( fqn );
    }

    @Override
    public PropertyType getPropertyType( UUID propertyTypeId ) {
        return propertyTypesById.get( propertyTypeId );
    }

    @Override
    public PropertyType getPropertyType( FullQualifiedName fqn ) {
        return getPropertyType( getPropertyTypeId( fqn ) );
    }

    @Deprecated
    @Override
    public Map<String, UUID> getLegacyPropertyTypeIds( Set<String> propertyTypeFqns ) {
        Map<String, UUID> propertyTypeMap = Maps.newHashMapWithExpectedSize( propertyTypeFqns.size() );

        for ( String fqnString : propertyTypeFqns ) {
            propertyTypeMap.put( fqnString, propertyTypeIdsByFQN.get( new FullQualifiedName( fqnString ) ) );
        }
        return propertyTypeMap;
    }

    @Override
    public Map<FullQualifiedName, UUID> getPropertyTypeIds( Set<FullQualifiedName> propertyTypeFqns ) {
        Map<FullQualifiedName, UUID> propertyTypeMap = Maps.newHashMapWithExpectedSize( propertyTypeFqns.size() );

        for ( FullQualifiedName fqn : propertyTypeFqns ) {
            propertyTypeMap.put( fqn, propertyTypeIdsByFQN.get( fqn ) );
        }

        return propertyTypeMap;
    }

}
