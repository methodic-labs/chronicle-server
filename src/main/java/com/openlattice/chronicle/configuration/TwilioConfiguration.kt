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

data class TwilioConfiguration  constructor(
    val sid: String,
    val token: String,
    val fromPhone: String
): Configuration {

    companion object {
        @JvmStatic
        val key = SimpleConfigurationKey(CONFIG_FILE_NAME)
    }

    override fun getKey(): ConfigurationKey {
        return TwilioConfiguration.key
    }
}