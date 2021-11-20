package com.openlattice.chronicle.services.entitysets;

import com.dataloom.streams.StreamUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.openlattice.apps.App;
import com.openlattice.apps.AppApi;
import com.openlattice.apps.UserAppConfig;
import com.openlattice.authorization.securable.AbstractSecurableObject;
import com.openlattice.chronicle.constants.*;
import com.openlattice.chronicle.data.*;
import com.openlattice.chronicle.services.ApiCacheManager;
import com.openlattice.chronicle.services.edm.EdmCacheManager;
import com.openlattice.chronicle.util.ChronicleServerUtil;
import com.openlattice.client.ApiClient;
import com.openlattice.collections.CollectionTemplateType;
import com.openlattice.collections.CollectionsApi;
import com.openlattice.collections.EntitySetCollection;
import com.openlattice.collections.EntityTypeCollection;
import com.openlattice.data.DataApi;
import com.openlattice.data.requests.EntitySetSelection;
import com.openlattice.data.requests.FileType;
import com.openlattice.entitysets.EntitySetsApi;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.openlattice.chronicle.constants.AppComponent.CHRONICLE;
import static com.openlattice.chronicle.constants.AppComponent.CHRONICLE_DATA_COLLECTION;
import static com.openlattice.chronicle.constants.AppComponent.CHRONICLE_SURVEYS;
import static com.openlattice.chronicle.constants.EdmConstants.*;
import static com.openlattice.chronicle.util.ChronicleServerUtil.getFirstUUIDOrNull;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public class EntitySetIdsService implements EntitySetIdsManager {
    protected static final Logger logger = LoggerFactory.getLogger( EntitySetIdsService.class );

    private final long ENTITY_SETS_REFRESH_INTERVAL     = 15 * 60 * 1000; // 15 minutes
    private final long PARTICIPANTS_ES_REFRESH_INTERVAL = 60 * 1000; // i minute

    // appName -> orgId -> templateName -> entitySetID
    private final Map<AppComponent, Map<UUID, Map<CollectionTemplateTypeName, UUID>>> entitySetIdsByOrgId = Maps
            .newHashMap();

    private final Map<String, UUID> legacyEntitySetIds = Maps.newHashMap();

    // appComponent -> orgId -> settings
    private final Map<AppComponent, Map<UUID, Map<String, Object>>> appSettings = Maps.newHashMap();

    private final ApiCacheManager apiCacheManager;
    private final EdmCacheManager edmCacheManager;

    public EntitySetIdsService( ApiCacheManager apiCacheManager, EdmCacheManager edmCacheManager )
            throws ExecutionException {
        this.apiCacheManager = apiCacheManager;
        this.edmCacheManager = edmCacheManager;

        ApiClient prodApiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
        EntitySetsApi entitySetsApi = prodApiClient.getEntitySetsApi();

        // get legacy entity set ids
        Map<String, UUID> entitySetIds = entitySetsApi.getEntitySetIds( LEGACY_ENTITY_SET_NAMES );
        this.legacyEntitySetIds.putAll( entitySetIds );

        refreshOrgsEntitySets();
        refreshLegacyParticipantEntitySetIds();
    }

    @Scheduled( fixedRate = ENTITY_SETS_REFRESH_INTERVAL )
    public void refreshOrgsEntitySets() throws ExecutionException {
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

            if ( configs.isEmpty() ) {
                return;
            }

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

            // orgId -> settings
            Map<UUID, Map<String, Object>> settings = Maps.newHashMap();

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

                settings.put(userAppConfig.getOrganizationId(), userAppConfig.getSettings());
            } );

            appSettings.put( appComponent, settings );
            entitySets.put( appComponent, orgEntitySetMap );
        } );

        entitySetIdsByOrgId.putAll( entitySets );
    }

    @Deprecated
    @Scheduled( fixedRate = PARTICIPANTS_ES_REFRESH_INTERVAL )
    public void refreshLegacyParticipantEntitySetIds() {
        logger.info( "refreshing participant entityset ids for legacy studies" );

        try {
            ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
            DataApi dataApi = apiClient.getDataApi();
            EntitySetsApi entitySetsApi = apiClient.getEntitySetsApi();

            UUID studiesEntitySetId = legacyEntitySetIds.get( STUDY_ES );

            Iterable<SetMultimap<FullQualifiedName, Object>> studies = dataApi.loadSelectedEntitySetData(
                    studiesEntitySetId,
                    new EntitySetSelection(
                            Optional.of( ImmutableSet.of( edmCacheManager.getPropertyTypeId( STRING_ID_FQN ) ) )
                    ),
                    FileType.json
            );

            Set<String> entitySetNames = StreamUtil.stream( studies )
                    .map( study -> getFirstUUIDOrNull( Multimaps.asMap( study ), STRING_ID_FQN ) )
                    .filter( Objects::nonNull )
                    .map( ChronicleServerUtil::getParticipantEntitySetName )
                    .collect( Collectors.toSet() );

            Map<String, UUID> entitySetIds = entitySetsApi.getEntitySetIds( entitySetNames );

            this.legacyEntitySetIds.putAll( entitySetIds );

            logger.info( "loaded {} legacy participant entity set ids", entitySetIds.size() );

        } catch ( Exception e ) {
            logger.error( "error refreshing legacy participant entityset ids" );
        }
    }

    @Override
    public Map<AppComponent, Map<UUID, Map<CollectionTemplateTypeName, UUID>>> getEntitySetIdsByOrgId() {
        return entitySetIdsByOrgId;
    }

    // app settings
    @Override
    public Map<String, Object> getOrgAppSettings(AppComponent appComponent, UUID organizationId) {
        return appSettings.getOrDefault( appComponent, ImmutableMap.of() ).getOrDefault( organizationId, ImmutableMap.of() );
    }

    @Override
    public Map<String, Object> getOrgAppSettings(String appName, UUID organizationId) {
        AppComponent component = AppComponent.fromString( appName );
        return getOrgAppSettings( component, organizationId );
    }


    @Override public EntitySetsConfig getEntitySetsConfig( UUID organizationId, UUID studyId, Set<AppComponent> components ) {
        if (organizationId == null) {
            return getChronicleLegacyAppConfig( studyId );
        }
        return getChronicleAppConfig( organizationId, components );
    }

    // helpers

    private ChronicleAppConfig getChronicleAppConfig(
            UUID organizationId, Set<AppComponent> components ) {
        ensureOrganizationHasComponents( organizationId, components);

        Map<CollectionTemplateTypeName, UUID> coreEntitySets = getEntitySetIdsByOrgId().get( CHRONICLE ).get( organizationId );
        Map<CollectionTemplateTypeName, UUID> dataCollectionEntitySets = getEntitySetIdsByOrgId().getOrDefault( CHRONICLE_DATA_COLLECTION, ImmutableMap.of() ).getOrDefault( organizationId, ImmutableMap.of() );
        Map<CollectionTemplateTypeName, UUID> questionnairesEntitySets = getEntitySetIdsByOrgId().getOrDefault( CHRONICLE_SURVEYS, ImmutableMap.of() ).getOrDefault( organizationId, ImmutableMap.of() );

        return new ChronicleAppConfig( coreEntitySets, dataCollectionEntitySets, questionnairesEntitySets );
    }

    private LegacyChronicleAppConfig getChronicleLegacyAppConfig( UUID studyId ) {
        return new LegacyChronicleAppConfig( legacyEntitySetIds, studyId );
    }

    private void ensureOrganizationHasComponents(UUID organizationId, Set<AppComponent> components) {
        components.forEach( appComponent -> {
            Map<UUID, Map<CollectionTemplateTypeName, UUID>> orgComponents = getEntitySetIdsByOrgId().getOrDefault( appComponent, ImmutableMap.of() );

            Preconditions.checkArgument(
                    orgComponents.containsKey( organizationId ),
                    "App component %s is not installed for organization %s",
                    appComponent,
                    organizationId
            );
        } );
    }
}
