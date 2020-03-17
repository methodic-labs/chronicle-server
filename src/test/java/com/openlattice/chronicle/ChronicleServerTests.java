package com.openlattice.chronicle;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.SetMultimap;
import com.google.common.eventbus.EventBus;
import com.openlattice.chronicle.configuration.ChronicleConfiguration;
import com.openlattice.chronicle.services.ChronicleService;
import com.openlattice.chronicle.services.ChronicleServiceImpl;
import com.openlattice.client.ApiClient;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.data.DataApi;
import com.openlattice.data.DeleteType;
import com.openlattice.edm.EdmApi;
import com.openlattice.entitysets.EntitySetsApi;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.inject.Inject;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class ChronicleServerTests {

    private final String DATA_ENTITY_SET_NAME = "chronicle_app_data";
    private static final String AUTH_TOKEN = ""; //replace this with a valid auth0_token


    private final String PARTICIPANT_ID       = "participant1";
    private final String DEVICE_ID              = "30d34cef1b0052e8";
    private final UUID   STUDY_ID             = UUID.fromString( "36ba6fab-76fa-4fe4-ad65-df4eae1f307a" );
    private final UUID   PARTICIPANT_EK_ID    = UUID.fromString( "00c60000-0000-0000-8000-000000000004" );

    private static final FullQualifiedName DURATION_FQN = new FullQualifiedName( "general.Duration" );
    private static final FullQualifiedName FULLNAME_FQN = new FullQualifiedName( "general.fullname" );
    private static final FullQualifiedName START_TIME_FQN = new FullQualifiedName( "ol.datetimestart" );
    private static final FullQualifiedName END_TIME_FQN = new FullQualifiedName( "general.EndTime" );
    private static final FullQualifiedName RECORD_TYPE_FQN = new FullQualifiedName( "ol.recordtype" );

    private UUID dataEntitySetId;
    private static UUID durationPTID;
    private static UUID fullNamePTID;
    private static  UUID startTimePTID;
    private static UUID endTimePTID;
    private static UUID recordTypePTID;

    private static ApiClient apiClient;

    @Inject
    private static EventBus eventBus;

    private ChronicleConfiguration configuration = new ChronicleConfiguration( "user", "password" );
    private ChronicleService chronicleService = new ChronicleServiceImpl( eventBus, configuration );

    public ChronicleServerTests() throws ExecutionException {
    }

    @BeforeClass
    public static void initialize() throws ExecutionException {

        apiClient = new ApiClient( RetrofitFactory.Environment.LOCAL, () -> AUTH_TOKEN );
        EdmApi edmApi = apiClient.getEdmApi();

        durationPTID = edmApi.getPropertyTypeId( DURATION_FQN.getNamespace(), DURATION_FQN.getName() );
        fullNamePTID = edmApi.getPropertyTypeId( FULLNAME_FQN.getNamespace(), FULLNAME_FQN.getName() );
        startTimePTID = edmApi.getPropertyTypeId( START_TIME_FQN.getNamespace(), START_TIME_FQN.getName() );
        endTimePTID = edmApi.getPropertyTypeId( END_TIME_FQN.getNamespace(), END_TIME_FQN.getName() );
        recordTypePTID = edmApi.getPropertyTypeId( RECORD_TYPE_FQN.getNamespace(), RECORD_TYPE_FQN.getName() );

    }

    @Test
    public void testGetParticipantData() throws ExecutionException {

        DataApi dataApi = apiClient.getDataApi();
        EntitySetsApi entitySetsApi = apiClient.getEntitySetsApi();

        dataEntitySetId = entitySetsApi.getEntitySetId( DATA_ENTITY_SET_NAME );
        dataApi.deleteAllEntitiesFromEntitySet(  dataEntitySetId, DeleteType.Hard);

        // try fetching empty participant data.
        // expected: an empty collection
        Iterable<Map<String, Set<Object>>> participantData = chronicleService.getAllParticipantData( STUDY_ID, PARTICIPANT_EK_ID, AUTH_TOKEN );
        Assert.assertEquals( 0, Iterators.size( participantData.iterator() ) );

        // upload data
        List<SetMultimap<UUID, Object>> data = createMockData(10);
        chronicleService.logData( STUDY_ID, PARTICIPANT_ID, DEVICE_ID, data );

        participantData = chronicleService.getAllParticipantData( STUDY_ID, PARTICIPANT_EK_ID, AUTH_TOKEN );
        Assert.assertEquals(data.size(), Iterators.size(participantData.iterator()));

        // load more data
        List<SetMultimap<UUID, Object>> additionalData = createMockData( 30 );
        chronicleService.logData( STUDY_ID, PARTICIPANT_ID, DEVICE_ID, additionalData );

        participantData = chronicleService.getAllParticipantData( STUDY_ID, PARTICIPANT_EK_ID, AUTH_TOKEN );
        Assert.assertEquals( data.size() + additionalData.size(), Iterators.size( participantData.iterator() ) );


        // test with invalid auth token
        try {
            participantData = chronicleService.getAllParticipantData( STUDY_ID, PARTICIPANT_EK_ID, "token" );
            Assert.fail();
        } catch ( Exception ignored ) {}

    }

    private List<SetMultimap<UUID, Object>> createMockData(int size) {

        List<String> appPackages = List.of( "com.youtube", "com.facebook", "com.random" );
        List<String> recordTypes = List.of( "Usage Stat",  "Move to foreground", "Move to background" );

        List<SetMultimap<UUID, Object>> data = new ArrayList<>(  );
        for (int i = 0; i < size; i++) {
            SetMultimap<UUID, Object> dataItem = HashMultimap.create();
            dataItem.put( fullNamePTID, getRandomString(appPackages) );
            dataItem.put( durationPTID, Long.parseLong( "20" ) );
            dataItem.put( startTimePTID, OffsetDateTime.now().minusHours( 3 ) );
            dataItem.put( endTimePTID, OffsetDateTime.now() );
            dataItem.put(recordTypePTID, getRandomString( recordTypes ));

            data.add( dataItem );
        }

        return data;
    }

    private Object getRandomString( List<String> elements ) {
        Random random = new Random(  );
        int index = random.nextInt( elements.size() );
        return elements.get( index );
    }

}
