package com.openlattice.chronicle.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kryptnostic.rhizome.configuration.Configuration;
import com.kryptnostic.rhizome.configuration.ConfigurationKey;
import com.kryptnostic.rhizome.configuration.SimpleConfigurationKey;
import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration;

@ReloadableConfiguration( uri = "twilio.yaml" )
public class TwilioConfiguration implements Configuration {

    protected static ConfigurationKey key = new SimpleConfigurationKey(
            "twilio.yaml" );

    private static final String SID_PROPERTY   = "sid";
    private static final String TOKEN_PROPERTY = "token";
    private static final String FROM_PHONE     = "fromPhone";

    private final String sid;
    private final String token;
    private final String fromPhone;

    @JsonCreator
    public TwilioConfiguration(
            @JsonProperty( SID_PROPERTY ) String sid,
            @JsonProperty( TOKEN_PROPERTY ) String token,
            @JsonProperty( FROM_PHONE ) String fromPhone ) {
        this.sid = sid;
        this.token = token;
        this.fromPhone = fromPhone;
    }

    @JsonProperty( SID_PROPERTY )
    public String getSid() {
        return sid;
    }

    @JsonProperty( TOKEN_PROPERTY )
    public String getToken() {
        return token;
    }

    @JsonProperty( FROM_PHONE )
    public String getFromPhone() {
        return fromPhone;
    }

    @Override
    @JsonIgnore
    public ConfigurationKey getKey() {
        return key;
    }

    @JsonIgnore
    public static ConfigurationKey key() {
        return key;
    }
}
