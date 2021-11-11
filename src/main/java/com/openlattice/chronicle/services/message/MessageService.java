package com.openlattice.chronicle.services.message;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.openlattice.chronicle.constants.EdmConstants;
import com.openlattice.chronicle.constants.MessageOutcome;
import com.openlattice.chronicle.data.ChronicleCoreAppConfig;
import com.openlattice.chronicle.data.MessageDetails;
import com.openlattice.chronicle.services.ApiCacheManager;
import com.openlattice.chronicle.services.edm.EdmCacheManager;
import com.openlattice.chronicle.services.enrollment.EnrollmentManager;
import com.openlattice.chronicle.services.entitysets.EntitySetIdsManager;
import com.openlattice.chronicle.services.twilio.TwilioManager;
import com.openlattice.client.ApiClient;
import com.openlattice.data.DataApi;
import com.openlattice.data.DataAssociation;
import com.openlattice.data.DataGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.openlattice.chronicle.util.ChronicleServerUtil.getParticipantEntitySetName;

public class MessageService implements MessageManager {
    protected static final Logger logger = LoggerFactory.getLogger( MessageService.class );

    private final ApiCacheManager       apiCacheManager;
    private final EdmCacheManager       edmCacheManager;
    private final EnrollmentManager     enrollmentManager;
    private final EntitySetIdsManager   entitySetIdsManager;
    private final TwilioManager         twilioManager;

    public MessageService(
            ApiCacheManager apiCacheManager,
            EdmCacheManager edmCacheManager,
            EnrollmentManager enrollmentManager,
            EntitySetIdsManager entitySetIdsManager,
            TwilioManager twilioManager ) {

        this.edmCacheManager = edmCacheManager;
        this.enrollmentManager = enrollmentManager;
        this.entitySetIdsManager = entitySetIdsManager;
        this.apiCacheManager = apiCacheManager;
        this.twilioManager = twilioManager;
    }

    public void sendMessage(UUID organizationId, UUID studyId, String participantId, Map<String, String> messageDetails) {
        MessageDetails message = new MessageDetails(
                messageDetails.getOrDefault( "type", "" ),
                OffsetDateTime.now(),
                participantId,
                messageDetails.get( "phone" ),
                messageDetails.get( "url" )
        );
        MessageOutcome outcome = twilioManager.sendMessage( participantId, message );
        try {
            recordMessageSent( organizationId, studyId, outcome );
            logger.info( "Recorded notification {} sent", outcome.getSid() );
        } catch ( ExecutionException e ) {
            logger.error( "Unable to record notification sent for SID {}", outcome.getSid(), e );
        }
    }

    public void recordMessageSent( UUID organizationId, UUID studyId, MessageOutcome messageOutcome ) throws ExecutionException {

        ApiClient apiClient = apiCacheManager.prodApiClientCache.get( ApiClient.class );
        DataApi dataApi = apiClient.getDataApi();

        // entity set ids
        String participantES = getParticipantEntitySetName( studyId );
        ChronicleCoreAppConfig coreAppConfig = entitySetIdsManager
                .getChronicleAppConfig( organizationId );
        ChronicleCoreAppConfig legacyAppConfig = entitySetIdsManager
                .getLegacyChronicleAppConfig( participantES );

        UUID messageESID = coreAppConfig.getMessagesEntitySetId();
        UUID sentToESID = legacyAppConfig.getSentToEntitySetId();
        UUID participantsESID = coreAppConfig.getParticipantEntitySetId();

        UUID participantEKID = enrollmentManager.getParticipantEntityKeyId( organizationId, studyId, messageOutcome.getParticipantId() );

        ListMultimap<UUID, Map<UUID, Set<Object>>> entities = ArrayListMultimap.create();
        ListMultimap<UUID, DataAssociation> associations = ArrayListMultimap.create();

        Map<UUID, Set<Object>> messageEntity = new HashMap<>();
        messageEntity.put( edmCacheManager.getPropertyTypeId( EdmConstants.TYPE_FQN ),
                ImmutableSet.of( messageOutcome.getMessageType() ) );

        messageEntity.put( edmCacheManager.getPropertyTypeId( EdmConstants.OL_ID_FQN ), ImmutableSet.of( messageOutcome.getSid() ) );
        messageEntity
                .put( edmCacheManager.getPropertyTypeId( EdmConstants.DATE_TIME_FQN ), ImmutableSet.of( messageOutcome.getDateTime() ) );
        messageEntity.put( edmCacheManager.getPropertyTypeId( EdmConstants.DELIVERED_FQN ), ImmutableSet.of( messageOutcome.isSuccess() ) );

        entities.put( messageESID, messageEntity );

        Map<UUID, Set<Object>> sentToEntity = new HashMap<>();
        sentToEntity.put( edmCacheManager.getPropertyTypeId( EdmConstants.DATE_TIME_FQN ),
                ImmutableSet.of( messageOutcome.getDateTime() ) );

        associations.put( sentToESID,
                new DataAssociation( messageESID,
                        Optional.of( 0 ),
                        Optional.empty(),
                        participantsESID,
                        Optional.empty(),
                        Optional.of( participantEKID ),
                        sentToEntity ) );
        DataGraph dataGraph = new DataGraph( entities, associations );
        dataApi.createEntityAndAssociationData( dataGraph );

    }
}
