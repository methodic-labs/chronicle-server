package com.openlattice.chronicle.services.download;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.openlattice.chronicle.services.CommonTasksManager;
import com.openlattice.client.ApiClient;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.data.requests.NeighborEntityDetails;
import com.openlattice.edm.EdmApi;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.set.EntitySetPropertyMetadata;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.entitysets.EntitySetsApi;
import com.openlattice.search.SearchApi;
import com.openlattice.search.requests.EntityNeighborsFilter;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.openlattice.chronicle.constants.AppComponent.CHRONICLE_DATA_COLLECTION;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.APPDATA;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.PREPROCESSED_DATA;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.RECORDED_BY;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.USED_BY;
import static com.openlattice.chronicle.constants.CollectionTemplateTypeName.USER_APPS;
import static com.openlattice.chronicle.constants.EdmConstants.DATA_ES;
import static com.openlattice.chronicle.constants.EdmConstants.PREPROCESSED_DATA_ES;
import static com.openlattice.chronicle.constants.EdmConstants.RECORDED_BY_ES;
import static com.openlattice.chronicle.constants.EdmConstants.TIMEZONE_FQN;
import static com.openlattice.chronicle.constants.EdmConstants.USED_BY_ES;
import static com.openlattice.chronicle.constants.EdmConstants.USER_APPS_ES;
import static com.openlattice.chronicle.constants.OutputConstants.APP_PREFIX;
import static com.openlattice.chronicle.constants.OutputConstants.DEFAULT_TIMEZONE;
import static com.openlattice.chronicle.constants.OutputConstants.USER_PREFIX;
import static com.openlattice.chronicle.util.ChronicleServerUtil.checkNotNullUUIDs;
import static com.openlattice.edm.EdmConstants.ID_FQN;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public class DataDownloadManagerImpl implements DataDownloadManager {
    protected static final Logger logger = LoggerFactory.getLogger( DataDownloadManagerImpl.class );

    private final CommonTasksManager commonTasksManager;

    public DataDownloadManagerImpl( CommonTasksManager commonTasksManager ) {
        this.commonTasksManager = commonTasksManager;
    }

    private Iterable<Map<String, Set<Object>>> getParticipantDataHelper(
            UUID organizationId,
            UUID studyId,
            UUID participantEntityKeyId,
            UUID srcESID,
            UUID edgeESID,
            String token ) {

        try {
            ApiClient apiClient = new ApiClient( RetrofitFactory.Environment.PROD_INTEGRATION, () -> token );
            EntitySetsApi entitySetsApi = apiClient.getEntitySetsApi();
            SearchApi searchApi = apiClient.getSearchApi();
            EdmApi edmApi = apiClient.getEdmApi();

            /*
             * 1. get the relevant EntitySets
             */

            UUID participantESID = commonTasksManager.getParticipantEntitySetId( organizationId, studyId );

            Map<UUID, EntitySet> entitySetsById = entitySetsApi.getEntitySetsById(
                    ImmutableSet.of( participantESID, srcESID, edgeESID )
            );

            EntitySet participantsES = entitySetsById.get( participantESID );
            EntitySet sourceES = entitySetsById.get( srcESID );
            EntitySet edgeES = entitySetsById.get( edgeESID );

            /*
             * 2. get all PropertyTypes and set up maps for easy lookups
             */

            Map<UUID, PropertyType> propertyTypesById = StreamSupport
                    .stream( edmApi.getPropertyTypes().spliterator(), false )
                    .collect( Collectors.toMap( PropertyType::getId, Function.identity() ) );

            Map<UUID, Map<UUID, EntitySetPropertyMetadata>> meta =
                    entitySetsApi.getPropertyMetadataForEntitySets( Set.of( sourceES.getId(), edgeES.getId() ) );

            Map<UUID, EntitySetPropertyMetadata> sourceMeta = meta.get( sourceES.getId() );
            Map<UUID, EntitySetPropertyMetadata> edgeMeta = meta.get( edgeES.getId() );

            /*
             * 3. get EntitySet primary keys, which are used later for filtering
             */

            Set<FullQualifiedName> sourceKeys = edmApi.getEntityType( sourceES.getEntityTypeId() )
                    .getKey()
                    .stream()
                    .map( propertyTypeId -> propertyTypesById.get( propertyTypeId ).getType() )
                    .collect( Collectors.toSet() );

            Set<FullQualifiedName> edgeKeys = edmApi.getEntityType( edgeES.getEntityTypeId() )
                    .getKey()
                    .stream()
                    .map( propertyTypeId -> propertyTypesById.get( propertyTypeId ).getType() )
                    .collect( Collectors.toSet() );

            /*
             * 4. perform filtered search to get participant neighbors
             */

            Map<UUID, List<NeighborEntityDetails>> participantNeighbors = searchApi.executeFilteredEntityNeighborSearch(
                    participantsES.getId(),
                    new EntityNeighborsFilter(
                            Set.of( participantEntityKeyId ),
                            Optional.of( ImmutableSet.of( sourceES.getId() ) ),
                            Optional.of( ImmutableSet.of( participantsES.getId() ) ),
                            Optional.of( ImmutableSet.of( edgeES.getId() ) )
                    )
            );

            /*
             * 5. filter and clean the data before sending it back
             */

            return participantNeighbors
                    .getOrDefault( participantEntityKeyId, List.of() )
                    .stream()
                    .filter( neighbor -> neighbor.getNeighborDetails().isPresent() )
                    .map( neighbor -> {

                        Map<FullQualifiedName, Set<Object>> entityData = neighbor.getNeighborDetails().get();
                        entityData.remove( ID_FQN );

                        ZoneId tz = ZoneId.of( entityData
                                .getOrDefault( TIMEZONE_FQN, ImmutableSet.of( DEFAULT_TIMEZONE ) )
                                .iterator()
                                .next()
                                .toString()
                        );

                        Map<String, Set<Object>> cleanEntityData = Maps.newHashMap();
                        entityData
                                .entrySet()
                                .stream()
                                .filter( entry -> !sourceKeys.contains( entry.getKey() ) )
                                .forEach( entry -> {
                                    Set<Object> values = entry.getValue();
                                    PropertyType propertyType = propertyTypesById.get(
                                            commonTasksManager.getPropertyTypeId( entry.getKey() )
                                    );
                                    String propertyTitle = sourceMeta.get( propertyType.getId() ).getTitle();
                                    if ( propertyType.getDatatype() == EdmPrimitiveTypeKind.DateTimeOffset ) {
                                        Set<Object> dateTimeValues = values
                                                .stream()
                                                .map( value -> {
                                                    try {
                                                        return OffsetDateTime
                                                                .parse( value.toString() )
                                                                .toInstant()
                                                                .atZone( tz )
                                                                .toOffsetDateTime()
                                                                .toString();
                                                    } catch ( Exception e ) {
                                                        return null;
                                                    }
                                                } )
                                                .filter( StringUtils::isNotBlank )
                                                .collect( Collectors.toSet() );
                                        cleanEntityData.put( APP_PREFIX + propertyTitle, dateTimeValues );
                                    } else {
                                        cleanEntityData.put( APP_PREFIX + propertyTitle, values );
                                    }
                                } );

                        neighbor.getAssociationDetails().remove( ID_FQN );
                        neighbor.getAssociationDetails()
                                .entrySet()
                                .stream()
                                .filter( entry -> !edgeKeys.contains( entry.getKey() ) )
                                .forEach( entry -> {
                                    UUID propertyTypeId = commonTasksManager.getPropertyTypeId( entry.getKey() );
                                    String propertyTitle = edgeMeta.get( propertyTypeId ).getTitle();
                                    cleanEntityData.put( USER_PREFIX + propertyTitle, entry.getValue() );
                                } );

                        return cleanEntityData;
                    } )
                    .collect( Collectors.toSet() );
        } catch ( Exception e ) {
            // since the response is meant to be a file download, returning "null" will respond with 200 and return
            // an empty file, which is not what we want. the request should not "succeed" when something goes wrong
            // internally. additionally, it doesn't seem right for the request to return a stacktrace. instead,
            // catching all exceptions and throwing a general exception here will result in a failed request with
            // a simple error message to indicate something went wrong during the file download.
            String errorMsg = "failed to download participant data";
            logger.error( errorMsg, e );
            throw new RuntimeException( errorMsg );
        }
    }

    @Override
    public Iterable<Map<String, Set<Object>>> getAllPreprocessedParticipantData(
            UUID organizationId,
            UUID studyId,
            UUID participatedInEntityKeyId,
            String token ) {

        UUID srcESID = commonTasksManager
                .getEntitySetId( organizationId, CHRONICLE_DATA_COLLECTION, PREPROCESSED_DATA, PREPROCESSED_DATA_ES );
        UUID edgeESID = commonTasksManager
                .getEntitySetId( organizationId, CHRONICLE_DATA_COLLECTION, RECORDED_BY, RECORDED_BY_ES );

        checkNotNullUUIDs( Sets.newHashSet( srcESID, edgeESID ) );

        return getParticipantDataHelper(
                organizationId,
                studyId,
                participatedInEntityKeyId,
                srcESID,
                edgeESID,
                token
        );
    }

    @Override
    public Iterable<Map<String, Set<Object>>> getAllParticipantData(
            UUID organizationId,
            UUID studyId,
            UUID participantEntityKeyId,
            String token ) {

        UUID srcESID = commonTasksManager.getEntitySetId( organizationId, CHRONICLE_DATA_COLLECTION, APPDATA, DATA_ES );
        UUID edgeESID = commonTasksManager
                .getEntitySetId( organizationId, CHRONICLE_DATA_COLLECTION, RECORDED_BY, RECORDED_BY_ES );

        checkNotNullUUIDs( Sets.newHashSet( srcESID, edgeESID ) );

        return getParticipantDataHelper(
                organizationId,
                studyId,
                participantEntityKeyId,
                srcESID,
                edgeESID,
                token
        );
    }

    @Override
    public Iterable<Map<String, Set<Object>>> getAllParticipantAppsUsageData(
            UUID organizationId,
            UUID studyId,
            UUID participantEntityKeyId,
            String token ) {

        UUID srcESID = commonTasksManager
                .getEntitySetId( organizationId, CHRONICLE_DATA_COLLECTION, USER_APPS, USER_APPS_ES );
        UUID edgeESID = commonTasksManager
                .getEntitySetId( organizationId, CHRONICLE_DATA_COLLECTION, USED_BY, USED_BY_ES );

        checkNotNullUUIDs( Sets.newHashSet( srcESID, edgeESID ) );

        return getParticipantDataHelper(
                organizationId,
                studyId,
                participantEntityKeyId,
                srcESID,
                edgeESID,
                token
        );
    }
}
