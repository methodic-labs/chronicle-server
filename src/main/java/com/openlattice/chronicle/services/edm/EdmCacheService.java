package com.openlattice.chronicle.services.edm;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.openlattice.chronicle.constants.*;
import com.openlattice.chronicle.services.ApiCacheManager;
import com.openlattice.chronicle.services.download.DataDownloadService;
import com.openlattice.client.ApiClient;
import com.openlattice.edm.EdmApi;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.entitysets.EntitySetsApi;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.openlattice.chronicle.constants.AppComponent.CHRONICLE;
import static com.openlattice.chronicle.constants.EdmConstants.ENTITY_SET_NAMES;
import static com.openlattice.chronicle.util.ChronicleServerUtil.getParticipantEntitySetName;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public class EdmCacheService implements EdmCacheManager {
    protected static final Logger logger = LoggerFactory.getLogger( EdmCacheService.class );

    private final Map<UUID, PropertyType>      propertyTypesById    = Maps.newHashMap();
    private final Map<String, UUID>            entitySetIdMap       = Maps.newHashMap();
    private final Map<FullQualifiedName, UUID> propertyTypeIdsByFQN = Maps.newHashMap();

    private final ApiCacheManager apiCacheManager;

    public EdmCacheService( ApiCacheManager apiCacheManager ) throws ExecutionException {
        this.apiCacheManager = apiCacheManager;

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

    public PropertyType getPropertyType (UUID propertyTypeId) {
        return propertyTypesById.get( propertyTypeId );
    }

    public PropertyType getPropertyType(FullQualifiedName fqn) {
        return getPropertyType( getPropertyTypeId( fqn ) );
    }

    @Override
    public Map<String, UUID> getPropertyTypeIds( Set<String> propertyTypeFqns ) {
        EdmApi edmApi;
        try {
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            edmApi = apiClient.getEdmApi();
        } catch ( ExecutionException e ) {
            logger.error( "Unable to load EdmApi" );
            return ImmutableMap.of();
        }

        return propertyTypeFqns.stream().map( FullQualifiedName::new ).map( fqn -> Pair
                .of( fqn.getFullQualifiedNameAsString(),
                        edmApi.getPropertyTypeId( fqn.getNamespace(), fqn.getName() ) ) )
                .collect( Collectors.toMap( Pair::getLeft, Pair::getRight ) );
    }
}
