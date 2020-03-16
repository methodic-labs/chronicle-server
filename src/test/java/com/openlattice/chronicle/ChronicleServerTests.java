package com.openlattice.chronicle;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.google.common.eventbus.EventBus;
import com.openlattice.chronicle.configuration.ChronicleConfiguration;
import com.openlattice.chronicle.data.ChronicleAppsUsageDetails;
import com.openlattice.chronicle.services.ChronicleService;
import com.openlattice.chronicle.services.ChronicleServiceImpl;
import com.openlattice.client.ApiClient;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.data.DataApi;
import com.openlattice.data.DeleteType;
import com.openlattice.data.requests.NeighborEntityDetails;
import com.openlattice.edm.EdmApi;
import com.openlattice.entitysets.EntitySetsApi;
import com.openlattice.search.SearchApi;
import com.openlattice.search.requests.EntityNeighborsFilter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.openlattice.edm.EdmConstants.ID_FQN;

public class ChronicleServerTests {

    protected static final Logger logger                  = LoggerFactory.getLogger( ChronicleServerTests.class );

    // property type FQNS
    private static final FullQualifiedName FULL_NAME_FQN   = new FullQualifiedName( "general.fullname" );
    private static final FullQualifiedName DURATION        = new FullQualifiedName( "general.Duration" );
    private static final FullQualifiedName DATE_LOGGED_FQN = new FullQualifiedName( "ol.datelogged" );
    private static final FullQualifiedName START_DATE_TIME = new FullQualifiedName( "ol.datetimestart" );
    private static final FullQualifiedName TITLE_FQN       = new FullQualifiedName( "ol.title" );
    private static final FullQualifiedName DATETIME_FQN    = new FullQualifiedName( "ol.datetime" );
    private static final FullQualifiedName USER_FQN        = new FullQualifiedName( "ol.user" );
    private static final FullQualifiedName RECORD_TYPE_FQN  = new FullQualifiedName( "ol.recordtype" );

    private static final String                       STUDY_ES_NAME             = "chronicle_study";
    private static final String                       DEVICES_ES_NAME           = "chronicle_device";
    private static final String                       APP_DATA_ES_NAME          = "chronicle_app_data";
    private static final String                       RECORDED_BY_ES_NAME       = "chronicle_recorded_by";
    private static final String                       CHRONICLE_USER_APPS       = "chronicle_user_apps";
    private static final String                       USED_BY_ES_NAME           = "chronicle_used_by";

    private static final String DEVICE1 = "30d34cef1b0052e8";
    private static final String DEVICE3 = "b368482c2607fe37";

    private static final UUID STUDY_ID = UUID.fromString( "36ba6fab-76fa-4fe4-ad65-df4eae1f307a" );
    private static final String PARTICIPANT1 = "participant1";
    private static final String UN_ENROLLED_PARTICIPANT = "participant2";
    private static final String PARTICIPANT3 = "participant3";

    private static final UUID participant1EntityKeyId   = UUID.fromString( "00c60000-0000-0000-8000-000000000004" );
    private static final UUID participant3EntityKeyId   = UUID.fromString( "3a870000-0000-0000-8000-000000000011" );

    private static final UUID device1EntityKeyId = UUID.fromString( "033d0000-0000-0000-8000-000000000004" );
    private static final UUID device3EntityKeyId = UUID.fromString( "a5210000-0000-0000-8000-000000000014" );

    // test app data
    private static final Pair<String, String>         CAMERA                    = Pair
            .of( "com.android.camera2", "Camera" );
    private static final Pair<String, String>         GMAIL                     = Pair
            .of( "Gmail", "com.google.android.gm" );
    private static final Pair<String, String>         YOUTUBE                   = Pair
            .of( "come.google.android.youtube", "YouTube" );
    private static final Pair<String, String>         CHROME                    = Pair
            .of( "com.android.chrome", "Chrome" );
    private static final Pair<String, String>         MAPS                      = Pair
            .of( "com.google.android.apps.maps", "Maps" );
    private static final Pair<String, String>         PHONE                     = Pair
            .of( "com.android.dialer", "Phone" );

    private static       Map<String, UUID>            entitySetNameIdMap        = new HashMap<>();
    private static       UUID                         fullNamePTID;
    private static       UUID                         durationPTID;
    private static       UUID                         titlePTID;
    private static       UUID                         startDateTimePTID;
    private static       UUID                         dateLoggedPTID;
    private static UUID recordTypePTID;

    private static DataApi       dataApi;
    private static EdmApi        edmApi;
    private static SearchApi     searchApi;
    private static EntitySetsApi entitySetsApi;

    ChronicleConfiguration configuration = new ChronicleConfiguration( "user", "password" );
    @Inject
    private EventBus eventBus;
    ChronicleService chronicleService = new ChronicleServiceImpl( eventBus, configuration );

    public ChronicleServerTests() throws ExecutionException {

    }

    @BeforeClass
    public static void chronicleServerTest() throws Exception {

        // replace this with an actual auth0_id token
        String token = "";

        LoadingCache<Class<?>, ApiClient> apiClientCache = CacheBuilder
                .newBuilder()
                .expireAfterWrite( Duration.ofHours( 10 ) )
                .build( new CacheLoader<Class<?>, ApiClient>() {
                    @Override
                    public ApiClient load( Class<?> key ) throws Exception {
                        // String token = MissionControl.getIdToken( TEST_USER, TEST_PASSWORD );
                        return new ApiClient( RetrofitFactory.Environment.TESTING, () -> token );
                    }
                } );
        ApiClient apiClient = apiClientCache.get( ApiClient.class );

        dataApi = apiClient.getDataApi();
        edmApi = apiClient.getEdmApi();
        entitySetsApi = apiClient.getEntitySetsApi();
        searchApi = apiClient.getSearchApi();

        setUpTestingEnvironment();
    }

    private static void deleteEntities() {

        Set<UUID> entitySetsIds = new HashSet<>();
        entitySetsIds.add( entitySetNameIdMap.get( CHRONICLE_USER_APPS ) );
        entitySetsIds.add( entitySetNameIdMap.get( APP_DATA_ES_NAME ) );
        // entitySetsIds.add( entitySetNameIdMap.get( RECORDED_BY_ES_NAME ) );
        // entitySetsIds.add( entitySetNameIdMap.get( USED_BY_ES_NAME ) );

        for ( UUID entitySetId : entitySetsIds ) {
            dataApi.deleteAllEntitiesFromEntitySet( entitySetId, DeleteType.Hard );
        }
    }

    private static List<NeighborEntityDetails> getParticipantNeighbors( UUID participantEKID, UUID studyID ) {

        String participantES = ChronicleServerUtil.getParticipantEntitySetName( studyID );
        UUID participantEntitySetId = entitySetsApi.getEntitySetId( participantES );
        UUID userAppsESID = entitySetNameIdMap.get( CHRONICLE_USER_APPS );
        UUID usedByESID = entitySetNameIdMap.get( USED_BY_ES_NAME );

        Map<UUID, List<NeighborEntityDetails>> participantNeighbors = searchApi
                .executeFilteredEntityNeighborSearch(
                        participantEntitySetId,
                        new EntityNeighborsFilter(
                                ImmutableSet.of( participantEKID ),
                                java.util.Optional.of( ImmutableSet.of( userAppsESID ) ),
                                java.util.Optional.of( ImmutableSet.of( participantEntitySetId ) ),
                                java.util.Optional.of( ImmutableSet.of( usedByESID ) )
                        )
                );

        return participantNeighbors.getOrDefault( participantEKID, List.of() );
    }

    private static void setUpTestingEnvironment() {
        getPropertyTypeIdsAndEntitySets();
        deleteEntities();
    }

    private static void getPropertyTypeIdsAndEntitySets() {

        fullNamePTID = edmApi.getPropertyTypeId( FULL_NAME_FQN.getNamespace(), FULL_NAME_FQN.getName() );
        durationPTID = edmApi.getPropertyTypeId( DURATION.getNamespace(), DURATION.getName() );
        titlePTID = edmApi.getPropertyTypeId( TITLE_FQN.getNamespace(), TITLE_FQN.getName() );
        startDateTimePTID = edmApi.getPropertyTypeId( START_DATE_TIME.getNamespace(), START_DATE_TIME.getName() );
        dateLoggedPTID = edmApi.getPropertyTypeId( DATE_LOGGED_FQN.getNamespace(), DATE_LOGGED_FQN.getName() );
        recordTypePTID = edmApi.getPropertyTypeId( RECORD_TYPE_FQN.getNamespace(), RECORD_TYPE_FQN.getName() );

        entitySetNameIdMap = entitySetsApi.getEntitySetIds( Set.of(
                CHRONICLE_USER_APPS,
                STUDY_ES_NAME,
                APP_DATA_ES_NAME,
                RECORDED_BY_ES_NAME,
                USED_BY_ES_NAME,
                DEVICES_ES_NAME
        ) );
    }

    @AfterClass
    public static void resetTestingEnvironment() {

    }

    @Test
    public void testInCompleteData() {
        deleteEntities();
        // incomplete items (items missing required properties like 'general.fullname') shouldn't be written to chronicle_user_apps
        // result: only complete items will be logged.

        List<SetMultimap<UUID, Object>> data = new ArrayList<>();
        SetMultimap<UUID, Object> item = createTestDataItem( YOUTUBE, OffsetDateTime.now(), OffsetDateTime.now(),Integer.toUnsignedLong( 1000 ) );
        data.add( item );

        // incomplete
        SetMultimap<UUID, Object> partialEntry = HashMultimap.create( item );
        partialEntry.removeAll( titlePTID );
        partialEntry.removeAll( fullNamePTID );
        data.add( partialEntry );

        chronicleService.logData( STUDY_ID, PARTICIPANT3, DEVICE3, data );

        // only 1 entry will be written to chronicle_user_apps and related associations
        Assert.assertEquals(1, getParticipantNeighbors( participant3EntityKeyId, STUDY_ID ).size() );
        Assert.assertEquals( 1, getDeviceNeighbors( device3EntityKeyId ).size() );
    }

    @Test
    public void testDataRecordType() {
        // data written in chronicle_user_apps and related associations should only be of type 'Usage Stat';
        // experiment: log data with 1 record type set to 'Usage Stat' and another record type set to any other value.

        // result: only the value set to 'Usage Stat should be written in user_apps entity set
        deleteEntities();

        List<SetMultimap<UUID, Object>> data = new ArrayList<>();
        SetMultimap<UUID, Object> item = createTestDataItem( YOUTUBE, OffsetDateTime.now(), OffsetDateTime.now(),Integer.toUnsignedLong( 1000 ) );
        data.add( item );

        SetMultimap<UUID, Object> partialEntry = HashMultimap.create(item);
        partialEntry.removeAll( recordTypePTID );
        partialEntry.put( recordTypePTID, "Move to background" );
        data.add( partialEntry );

        chronicleService.logData( STUDY_ID, PARTICIPANT3, DEVICE3, data );

        Assert.assertEquals( 1, getParticipantNeighbors( participant3EntityKeyId, STUDY_ID ).size() );
        Assert.assertEquals( 1, getDeviceNeighbors( device3EntityKeyId ).size() );
    }

    @Test
    public void testUniquenessWrtToAppName() {
        deleteEntities();

        // chronicle_user_apps entities are unique for each app
        // log 4 items with matching app name, and 1 item with a different app name:
        // expected entities created in chronicle_user_apps = 2

        List<SetMultimap<UUID, Object>> data = new ArrayList<>();

        SetMultimap<UUID, Object> item = HashMultimap.create();

        for ( int i = 0; i < 4; i++ ) {
             item = createTestDataItem(
                    GMAIL,
                    createDateTime( 21, 4, 4, 50 ),
                    createDateTime( 21, 4, 4, 30 ),
                    Long.parseLong( "2000" )
            );
            data.add( item );
        }

        SetMultimap<UUID, Object> anotherItem = HashMultimap.create(item);
        anotherItem.removeAll( fullNamePTID );
        anotherItem.put( fullNamePTID, YOUTUBE.getLeft() );
        data.add( anotherItem );

        chronicleService.logData( STUDY_ID, PARTICIPANT1, DEVICE1, data );

        List<NeighborEntityDetails> participantNeighbors = getParticipantNeighbors( participant1EntityKeyId, STUDY_ID );
        Assert.assertEquals( 2,  participantNeighbors.size());

        List<NeighborEntityDetails> deviceNeighbors = getDeviceNeighbors(device1EntityKeyId);
        Assert.assertEquals( 2, deviceNeighbors.size() );
    }

    @Test
    public void testZeroDurationProperty() {
        deleteEntities();

        // entities created in chronicle_user_apps should have general.duration property > 0
        List<SetMultimap<UUID, Object>> data = new ArrayList<>();

        for ( int i = 0; i < 10; i++ ) {
            SetMultimap<UUID, Object> item = createTestDataItem(
                    GMAIL,
                    createDateTime( 10, 6, 2, 12 ),
                    createDateTime( 10, 6, 2, 12 ),
                    Integer.toUnsignedLong( 0 )
            );
            data.add( item );

        }
        chronicleService.logData( STUDY_ID, PARTICIPANT1, DEVICE1, data );
        Assert.assertEquals( 0, getParticipantNeighbors( participant3EntityKeyId, STUDY_ID ).size());
        Assert.assertEquals( 0, getDeviceNeighbors(device1EntityKeyId ).size() );

    }

    @Test
    public void testUnEnrolledParticipant() {
        // a participant must be enrolled for data to be logged
        // experiment: un_enroll participant, then try logging data

        List<SetMultimap<UUID, Object>> data = new ArrayList<>();
        SetMultimap<UUID, Object> item = createTestDataItem(
                GMAIL,
                createDateTime( 30, 5, 1, 1 ),
                createDateTime( 13, 5, 2, 1 ),
                Integer.toUnsignedLong( 1000 )
        );
        data.add( item );

        Assert.assertEquals( 0, chronicleService.logData( STUDY_ID, UN_ENROLLED_PARTICIPANT, DEVICE1, data ).intValue());
    }

    @Test
    public void usedByAssociationUniquenessWrtDateLogged() {

        deleteEntities();

        // chronicle_used_by associations are unique for app + user + date logged
        // experiment: 2 matching items (w.r.t app + user + date), an additional item that only differs in the date logged
        // result: 2 used_by_associations

        List<SetMultimap<UUID, Object>> data = new ArrayList<>();
        SetMultimap<UUID, Object> item = HashMultimap.create();
        for ( int i = 0; i < 2; i++ ) {
            item = createTestDataItem(
                    GMAIL,
                    createDateTime( 30, 5, 1, 1 ),
                    createDateTime( 30, 5, 1, 1 ),
                    Long.parseLong( "1000" )
            );
            data.add( item );
        }

        SetMultimap<UUID, Object> anotherItem = HashMultimap.create(item);
        anotherItem.removeAll( dateLoggedPTID );
        anotherItem.put( dateLoggedPTID, createDateTime( 13, 5, 2, 1 ).toString() );
        data.add( anotherItem );

        chronicleService.logData( STUDY_ID, PARTICIPANT1, DEVICE1, data );

        Assert.assertEquals( 2,  getParticipantNeighbors( participant1EntityKeyId, STUDY_ID ).size() );
    }


    private List<NeighborEntityDetails> getDeviceNeighbors(UUID deviceEntityKeyId) {
        UUID recordedByESID = entitySetNameIdMap.get( RECORDED_BY_ES_NAME );
        UUID userAppsESID = entitySetNameIdMap.get( CHRONICLE_USER_APPS );
        UUID deviceESID = entitySetNameIdMap.get( DEVICES_ES_NAME );

        Map<UUID, List<NeighborEntityDetails>> neighbors = searchApi
                .executeFilteredEntityNeighborSearch(
                        entitySetNameIdMap.get( DEVICES_ES_NAME ),
                        new EntityNeighborsFilter(
                                Set.of( deviceEntityKeyId ),
                                Optional.of( Set.of( userAppsESID ) ),
                                Optional.of( Set.of( deviceESID ) ),
                                Optional.of( Set.of( recordedByESID ) )
                        )
                );


        return neighbors.getOrDefault( deviceEntityKeyId, List.of(  ) );
    }

    @Test
    public void testGetUserAppsData() {

        deleteEntities();

        // log date with date logged set to today
        Set<Pair<String, String>> testApps = Set.of( GMAIL, YOUTUBE, CAMERA, CHROME, MAPS, PHONE );
        List<SetMultimap<UUID, Object>> data = new ArrayList<>();
        for ( Pair<String, String> app : testApps ) {
            data.add( createTestDataItem(
                    app,
                    OffsetDateTime.now(),
                    OffsetDateTime.now(),
                    Long.parseLong( "2000" )
            ) );
        }

        Assert.assertEquals( chronicleService.logData( STUDY_ID, PARTICIPANT1, DEVICE1, data ).intValue(),  testApps.size());

        List<ChronicleAppsUsageDetails> appsUsageDetails = chronicleService
                .getParticipantAppsUsageData( STUDY_ID, PARTICIPANT1 );
        Assert.assertEquals(appsUsageDetails.size(), testApps.size());

        // validate data
        Set<String> appNames = testApps.stream().map( Pair::getRight ).collect( Collectors.toSet() );
        Set<String> packageNames = testApps.stream().map( Pair::getLeft ).collect( Collectors.toSet() );

        Set<String> resultPackageNames = appsUsageDetails.stream()
                .map( item -> item.getEntityDetails().get( FULL_NAME_FQN ).iterator().next().toString() ).collect(
                        Collectors.toSet() );
        Set<String> resultAppNames = appsUsageDetails.stream()
                .map( item -> item.getEntityDetails().get( TITLE_FQN ).iterator().next().toString() ).collect(
                        Collectors.toSet() );

        Assert.assertEquals( appNames, resultAppNames );
        Assert.assertEquals( packageNames, resultPackageNames );

    }

    @Test
    public void testUpdateUserAppsAssociations() {
        deleteEntities();

        Set<Pair<String, String>> testApps = Set.of( GMAIL, YOUTUBE, CAMERA, CHROME, MAPS, PHONE );
        List<SetMultimap<UUID, Object>> data = new ArrayList<>();
        for ( Pair<String, String> app : testApps ) {
            data.add( createTestDataItem(
                    app,
                    OffsetDateTime.now(),
                    OffsetDateTime.now(),
                    Long.parseLong( "2000" )
            ) );
        }

        chronicleService.logData( STUDY_ID, PARTICIPANT1, DEVICE1, data );

        // 1: get the associations and update them with ol.user property
        Map<UUID, Map<FullQualifiedName, Set<Object>>> associations = getUserAppsAssociationDetails( STUDY_ID,
                PARTICIPANT1 );

        List<String> userTypes = List.of( "child", "parent", "parent_and_child" );
        associations.forEach( ( entityKeyId, entityData ) -> {
            entityData.remove( ID_FQN );
            Set<Object> users = ChronicleServerTestUtils.getRandomElements( userTypes );
            entityData.put( USER_FQN, users );
        } );

        Assert.assertEquals( associations.size(),
                chronicleService.updateAppsUsageAssociationData( STUDY_ID, PARTICIPANT1, associations ).intValue() );

        // verify that the associations were correctly updated
        Map<UUID, Map<FullQualifiedName, Set<Object>>> updateResult = getUserAppsAssociationDetails( STUDY_ID,
                PARTICIPANT1 );
        updateResult.forEach( ( associationId, entityData ) -> {
            Assert.assertEquals( entityData.getOrDefault( USER_FQN, Set.of() ),
                    associations.get( associationId ).getOrDefault( USER_FQN, Set.of() ) );
            Assert.assertEquals( entityData.get( DATETIME_FQN ).toString(),
                    associations.get( associationId ).get( DATETIME_FQN ).toString() );
        } );
    }


    private Map<UUID, Map<FullQualifiedName, Set<Object>>> getUserAppsAssociationDetails(
            UUID studyId,
            String participant ) {
        List<ChronicleAppsUsageDetails> appsUsageDetails = chronicleService
                .getParticipantAppsUsageData( studyId, participant );

        return appsUsageDetails
                .stream()
                .collect( Collectors.toMap( item -> UUID
                                .fromString( item.getAssociationDetails().get( ID_FQN ).iterator().next().toString() ),
                        ChronicleAppsUsageDetails::getAssociationDetails ) );

    }

    private OffsetDateTime createDateTime( int day, int month, int hour, int minute ) {
        return OffsetDateTime
                .now()
                .withMinute( minute )
                .withHour( hour )
                .withMonth( month )
                .withDayOfMonth( day );
    }

    private SetMultimap<UUID, Object> createTestDataItem(
            Pair<String, String> userApp,
            OffsetDateTime startTime,
            OffsetDateTime dateLogged,
            Long duration
    ) {
        SetMultimap<UUID, Object> data = HashMultimap.create();
        data.put( fullNamePTID, userApp.getLeft() );
        data.put( titlePTID, userApp.getRight() );
        data.put( startDateTimePTID, startTime.toString() );
        data.put( dateLoggedPTID, dateLogged.toString() );
        data.put( durationPTID, duration );
        data.put( recordTypePTID, "Usage Stat" );

        return data;
    }

}
