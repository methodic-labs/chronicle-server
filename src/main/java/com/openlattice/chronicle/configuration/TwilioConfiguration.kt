package com.openlattice.chronicle.configuration

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.kryptnostic.rhizome.configuration.Configuration
import com.kryptnostic.rhizome.configuration.ConfigurationKey
import com.kryptnostic.rhizome.configuration.SimpleConfigurationKey
import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration

private const val CONFIG_FILE_NAME = "twilio.yaml"
@ReloadableConfiguration(uri = CONFIG_FILE_NAME)

class TwilioConfiguration @JsonCreator constructor(
    @JsonProperty val sid: String,
    @JsonProperty val token: String,
    @JsonProperty val fromPhone: String
) : Configuration {

    @JsonIgnore
    override fun getKey(): ConfigurationKey {
        return Companion.key
    }

    companion object {
        protected var key: ConfigurationKey = SimpleConfigurationKey(CONFIG_FILE_NAME)
        @JsonIgnore
        fun key(): ConfigurationKey {
            return key
        }
    }
}