package com.openlattice.chronicle.services;

import com.google.common.collect.ImmutableMap;
import com.openlattice.chronicle.constants.*;
import com.openlattice.chronicle.services.edm.EdmCacheService;
import com.openlattice.client.ApiClient;
import com.openlattice.entitysets.EntitySetsApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;

import static com.openlattice.chronicle.constants.AppComponent.CHRONICLE;
import static com.openlattice.chronicle.util.ChronicleServerUtil.getParticipantEntitySetName;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public class CommonTasksManager {
    protected static final Logger logger = LoggerFactory.getLogger( CommonTasksManager.class );

    private final ApiCacheManager       apiCacheManager;
    private final EdmCacheService       edmCacheService;
    private final ScheduledTasksManager scheduledTasksManager;

    public CommonTasksManager(
            ApiCacheManager apiCacheManager,
            EdmCacheService edmCacheService,
            ScheduledTasksManager scheduledTasksManager ) {
        this.apiCacheManager = apiCacheManager;
        this.edmCacheService = edmCacheService;
        this.scheduledTasksManager = scheduledTasksManager;
    }

    public UUID getEntitySetId(
            UUID organizationId,
            AppComponent appComponent,
            CollectionTemplateTypeName templateName,
            String entitySetName
    ) {

        if ( organizationId == null ) {
            return edmCacheService.getHistoricalEntitySetId( entitySetName );
        }

        Map<CollectionTemplateTypeName, UUID> templateEntitySetIdMap = scheduledTasksManager.getEntitySetIdsByOrgId()
                .getOrDefault( appComponent, ImmutableMap.of() )
                .getOrDefault( organizationId, ImmutableMap.of() );

        if ( templateEntitySetIdMap.isEmpty() ) {
            logger.error( "organization {} does not have app {} installed", organizationId, appComponent );
            return null;
        }

        if ( !templateEntitySetIdMap.containsKey( templateName ) ) {
            logger.error( "app {} does not have a template {} in its entityTypeCollection",
                    appComponent,
                    templateName );
            return null;
        }

        return templateEntitySetIdMap.get( templateName );
    }

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
        return scheduledTasksManager.getEntitySetIdsByOrgId()
                .getOrDefault( CHRONICLE, ImmutableMap.of() )
                .getOrDefault( organizationId, ImmutableMap.of() )
                .getOrDefault( CollectionTemplateTypeName.PARTICIPANTS, null );
    }

    public UUID getParticipantEntityKeyId( UUID organizationId, UUID studyId, String participantId ) {
        if ( organizationId != null ) {
            Map<UUID, Map<String, UUID>> participants = scheduledTasksManager.getStudyParticipantsByOrg()
                    .getOrDefault( organizationId, Map.of() );

            return participants.getOrDefault( studyId, Map.of() ).getOrDefault( participantId, null );
        }

        return scheduledTasksManager.getStudyParticipants().getOrDefault( studyId, Map.of() )
                .getOrDefault( participantId, null );
    }

    public UUID getStudyEntityKeyId( UUID organizationId, UUID studyId ) {
        if ( organizationId != null ) {
            return scheduledTasksManager.getStudyEntityKeyIdsByOrg().getOrDefault( organizationId, Map.of() )
                    .getOrDefault( studyId, null );
        }
        return scheduledTasksManager.getStudyEKIDById().getOrDefault( studyId, null );
    }

}
