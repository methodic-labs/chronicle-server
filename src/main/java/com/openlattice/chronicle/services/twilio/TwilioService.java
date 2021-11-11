package com.openlattice.chronicle.services.twilio;

import com.openlattice.chronicle.configuration.TwilioConfiguration;
import com.openlattice.chronicle.constants.MessageOutcome;
import com.openlattice.chronicle.constants.MessageDetails;
import com.twilio.Twilio;
import com.twilio.exception.ApiException;
import com.twilio.rest.api.v2010.account.Message;
import com.twilio.type.PhoneNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.OffsetDateTime;

public class TwilioService implements TwilioManager {
    protected static final Logger logger = LoggerFactory.getLogger( TwilioService.class );

    private final PhoneNumber fromPhoneNumber;

    private static final String URL = "{{URL}}";

    public TwilioService( TwilioConfiguration configuration ) {
        this.fromPhoneNumber = new PhoneNumber( configuration.getFromPhone() );
        Twilio.init( configuration.getSid(), configuration.getToken() );
    }

    public MessageOutcome sendMessage( String participantId, MessageDetails messageDetails) {
        String messageText = "Follow this link to enroll in Chronicle: {{URL}}".replace(URL, messageDetails.getUrl());

        try {
            Message message = Message
                    .creator( new PhoneNumber( messageDetails.getPhoneNumber() ), fromPhoneNumber, messageText )
                    .setStatusCallback( URI
                            .create( "https://api.openlattice.com/bifrost/messages/status" ) )
                    .create();

            return new MessageOutcome(
                    messageDetails.getMessageType(),
                    OffsetDateTime.now(),
                    participantId,
                    messageDetails.getUrl(),
                    !message.getStatus().equals(
                            Message.Status.FAILED ),
                    message.getSid() );

        } catch ( ApiException e ) {
            logger.error( "Unable to send message to {}", messageDetails.getPhoneNumber(), e );
            return new MessageOutcome(
                    messageDetails.getMessageType(),
                    OffsetDateTime.now(),
                    participantId,
                    messageDetails.getUrl(),
                    false,
                    "message not sent" );
        }
    }
}
