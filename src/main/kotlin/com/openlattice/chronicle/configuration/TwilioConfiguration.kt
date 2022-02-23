package com.openlattice.chronicle.configuration

import com.geekbeast.rhizome.configuration.Configuration
import com.geekbeast.rhizome.configuration.ConfigurationKey
import com.geekbeast.rhizome.configuration.SimpleConfigurationKey
import com.geekbeast.rhizome.configuration.configuration.annotation.ReloadableConfiguration

private const val CONFIG_FILE_NAME = "twilio.yaml"

@ReloadableConfiguration(uri = CONFIG_FILE_NAME)
data class TwilioConfiguration(
    val sid: String,
    val token: String,
    val fromPhone: String
) : Configuration {

    companion object {
        @JvmStatic
        val key = SimpleConfigurationKey(CONFIG_FILE_NAME)
    }

    override fun getKey(): ConfigurationKey {
        return TwilioConfiguration.key
    }
}