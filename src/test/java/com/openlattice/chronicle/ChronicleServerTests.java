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
    private static final String AUTH_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6ImFsZm9uY2VAb3BlbmxhdHRpY2UuY29tIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsInVzZXJfaWQiOiJnb29nbGUtb2F1dGgyfDEwODQ4MDI2NTc3ODY0NDk2MTU1NCIsImFwcF9tZXRhZGF0YSI6eyJyb2xlcyI6WyJBdXRoZW50aWNhdGVkVXNlciJdLCJhY3RpdmF0ZWQiOiJhY3RpdmF0ZWQifSwibmlja25hbWUiOiJhbGZvbmNlIiwicm9sZXMiOlsiQXV0aGVudGljYXRlZFVzZXIiXSwiaXNzIjoiaHR0cHM6Ly9vcGVubGF0dGljZS5hdXRoMC5jb20vIiwic3ViIjoiZ29vZ2xlLW9hdXRoMnwxMDg0ODAyNjU3Nzg2NDQ5NjE1NTQiLCJhdWQiOiJLVHpneXhzNktCY0pIQjg3MmVTTWUyY3BUSHpoeFM5OSIsImlhdCI6MTU4NDM3NzY2NSwiZXhwIjoxNTg0NDY0MDY1fQ.VqgGArT8h3FllfA-V0eYEMr7BQy1Tq8Kcf0W0C65s0o";


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

    private static ChronicleService chronicleService;

    private static ApiClient apiClient;

    @Inject
    private static EventBus eventBus;

    @BeforeClass
    public static void initialize() throws ExecutionException {
        ChronicleConfiguration configuration = new ChronicleConfiguration( "user", "password" );
        chronicleService = new ChronicleServiceImpl( eventBus, configuration );

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

        // upload data
        List<SetMultimap<UUID, Object>> data = createMockData(10);
        chronicleService.logData( STUDY_ID, PARTICIPANT_ID, DEVICE_ID, data );

        Iterable<Map<String, Set<Object>>> participantData = chronicleService.getAllParticipantData( STUDY_ID, PARTICIPANT_EK_ID, AUTH_TOKEN );

        Assert.assertEquals(10, Iterators.size(participantData.iterator()));
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
