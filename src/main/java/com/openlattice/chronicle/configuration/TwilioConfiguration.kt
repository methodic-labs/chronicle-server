package com.openlattice.chronicle.configuration

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.kryptnostic.rhizome.configuration.Configuration
import com.kryptnostic.rhizome.configuration.ConfigurationKey
import com.kryptnostic.rhizome.configuration.SimpleConfigurationKey
import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration

@ReloadableConfiguration(uri = "twilio.yaml")
class TwilioConfiguration @JsonCreator constructor(
    @get:JsonProperty(SID_PROPERTY)
    @param:JsonProperty(SID_PROPERTY) val sid: String,
    @get:JsonProperty(TOKEN_PROPERTY)
    @param:JsonProperty(TOKEN_PROPERTY) val token: String,
    @get:JsonProperty(FROM_PHONE)
    @param:JsonProperty(FROM_PHONE) val fromPhone: String
) : Configuration {

    @JsonIgnore
    override fun getKey(): ConfigurationKey {
        return Companion.key
    }

    companion object {
        protected var key: ConfigurationKey = SimpleConfigurationKey(
            "twilio.yaml"
        )
        private const val SID_PROPERTY = "sid"
        private const val TOKEN_PROPERTY = "token"
        private const val FROM_PHONE = "fromPhone"
        @JsonIgnore
        fun key(): ConfigurationKey {
            return key
        }
    }
}