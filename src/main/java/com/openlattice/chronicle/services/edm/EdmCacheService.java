package com.openlattice.chronicle.services.edm;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.openlattice.apps.App;
import com.openlattice.apps.AppApi;
import com.openlattice.apps.UserAppConfig;
import com.openlattice.authorization.securable.AbstractSecurableObject;
import com.openlattice.chronicle.constants.*;
import com.openlattice.chronicle.services.ApiCacheManager;
import com.openlattice.client.ApiClient;
import com.openlattice.collections.CollectionTemplateType;
import com.openlattice.collections.CollectionsApi;
import com.openlattice.collections.EntitySetCollection;
import com.openlattice.collections.EntityTypeCollection;
import com.openlattice.edm.EdmApi;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.entitysets.EntitySetsApi;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.*;
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

    private final long ENTITY_SETS_REFRESH_INTERVAL = 15 * 60 * 1000; // 15 minutes

    // appName -> orgId -> templateName -> entitySetID
    private final Map<AppComponent, Map<UUID, Map<CollectionTemplateTypeName, UUID>>> entitySetIdsByOrgId  = Maps
            .newHashMap();
    private final Map<UUID, PropertyType>                                             propertyTypesById    = Maps
            .newHashMap();
    private final Map<String, UUID>                                                   entitySetIdMap       = Maps
            .newHashMap();
    private final Map<FullQualifiedName, UUID>                                        propertyTypeIdsByFQN = Maps
            .newHashMap();

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

        initializeEntitySets();
    }

    @Scheduled( fixedRate = ENTITY_SETS_REFRESH_INTERVAL )
    public void initializeEntitySets() throws ExecutionException {
        logger.info( "Refreshing entity set ids map" );

        ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
        AppApi appApi = apiClient.getAppApi();
        CollectionsApi collectionsApi = apiClient.getCollectionsApi();

        // create a app -> appId mapping
        Map<AppComponent, UUID> appNameIdMap = Maps.newHashMapWithExpectedSize( AppComponent.values().length );
        for ( AppComponent component : AppComponent.values() ) {
            App app = appApi.getAppByName( component.toString() );
            appNameIdMap.put( component, app.getId() );
        }

        Map<AppComponent, Map<UUID, Map<CollectionTemplateTypeName, UUID>>> entitySets = new HashMap<>();

        // get configs for each app
        appNameIdMap.forEach( ( appComponent, appId ) -> {
            List<UserAppConfig> configs = appApi.getAvailableAppConfigs( appId );

            if ( configs.isEmpty() )
                return;

            // get EntityTypeCollection associated with app
            EntitySetCollection entitySetCollection = collectionsApi
                    .getEntitySetCollection( configs.get( 0 ).getEntitySetCollectionId() );
            EntityTypeCollection entityTypeCollection = collectionsApi
                    .getEntityTypeCollection( entitySetCollection.getEntityTypeCollectionId() );

            // create mapping from templateTypeName -> templateTypeId
            Map<String, UUID> templateTypeNameIdMap = entityTypeCollection
                    .getTemplate()
                    .stream()
                    .collect( Collectors.toMap( CollectionTemplateType::getName, AbstractSecurableObject::getId ) );

            // for each config map orgId -> templateTypeName -> ESID
            Map<UUID, Map<CollectionTemplateTypeName, UUID>> orgEntitySetMap = new HashMap<>();

            configs.forEach( userAppConfig -> {
                Map<UUID, UUID> templateTypeIdESIDMap = collectionsApi
                        .getEntitySetCollection( userAppConfig.getEntitySetCollectionId() ).getTemplate();

                // iterate over templateTypeName enums and create mapping templateTypeName -> entitySetId
                Map<CollectionTemplateTypeName, UUID> templateTypeNameESIDMap = Maps
                        .newHashMapWithExpectedSize( templateTypeIdESIDMap.size() );

                for ( CollectionTemplateTypeName templateTypeName : CollectionTemplateTypeName.values() ) {
                    UUID templateTypeId = templateTypeNameIdMap.get( templateTypeName.toString() );

                    if ( templateTypeId != null ) {
                        templateTypeNameESIDMap.put( templateTypeName, templateTypeIdESIDMap.get( templateTypeId ) );
                    }
                }
                orgEntitySetMap.put( userAppConfig.getOrganizationId(), templateTypeNameESIDMap );
            } );

            entitySets.put( appComponent, orgEntitySetMap );
        } );

        entitySetIdsByOrgId.putAll( entitySets );
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

    @Override
    public Map<AppComponent, Map<UUID, Map<CollectionTemplateTypeName, UUID>>> getEntitySetIdsByOrgId() {
        return entitySetIdsByOrgId;
    }

    @Override
    public UUID getParticipantEntitySetId( UUID organizationId, UUID studyId ) {
        if ( organizationId == null ) {
            try {
                ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
                EntitySetsApi entitySetsApi = apiClient.getEntitySetsApi();

                return entitySetsApi.getEntitySetId( getParticipantEntitySetName( studyId ) );
            } catch ( Exception e ) {
                logger.error( " unable to load apis" );
            }
        }
        return getEntitySetIdsByOrgId()
                .getOrDefault( CHRONICLE, ImmutableMap.of() )
                .getOrDefault( organizationId, ImmutableMap.of() )
                .getOrDefault( CollectionTemplateTypeName.PARTICIPANTS, null );
    }

    @Override
    public UUID getEntitySetId( String entitySetName ) {
        return this.entitySetIdMap.getOrDefault( entitySetName, null );
    }

    @Override
    public UUID getEntitySetId(
            UUID organizationId,
            AppComponent appComponent,
            CollectionTemplateTypeName templateName,
            String entitySetName
    ) {

        if ( organizationId == null ) {
            return getEntitySetId( entitySetName );
        }
        return getEntitySetId( organizationId, appComponent, templateName );
    }

    @Override
        // get entity set id from a app configs context
    public UUID getEntitySetId (
            UUID organizationId,
            AppComponent appComponent,
            CollectionTemplateTypeName templateTypeName
    ) {
        Map<CollectionTemplateTypeName, UUID> templateEntitySetIdMap = getEntitySetIdsByOrgId()
                .getOrDefault( appComponent, ImmutableMap.of() )
                .getOrDefault( organizationId, ImmutableMap.of() );

        if ( templateEntitySetIdMap.isEmpty() ) {
            logger.error( "organization {} does not have app {} installed", organizationId, appComponent );
            return null;
        }

        if ( !templateEntitySetIdMap.containsKey( templateTypeName ) ) {
            logger.error( "app {} does not have a template {} in its entityTypeCollection",
                    appComponent,
                    templateTypeName );
            return null;
        }

        return templateEntitySetIdMap.get( templateTypeName );
    }
}