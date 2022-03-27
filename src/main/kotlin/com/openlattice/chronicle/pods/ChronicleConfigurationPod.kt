package com.openlattice.chronicle.pods

import com.geekbeast.jdbc.DataSourceManager
import com.geekbeast.mail.MailServiceConfig
import com.geekbeast.rhizome.pods.ConfigurationLoader
import com.openlattice.chronicle.configuration.ChronicleConfiguration
import com.openlattice.chronicle.configuration.TwilioConfiguration
import com.openlattice.chronicle.storage.StorageResolver
import com.twilio.Twilio
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Configuration
class ChronicleConfigurationPod {
    @Inject
    private lateinit var dataSourceManager: DataSourceManager

    @Inject
    private lateinit var configurationLoader: ConfigurationLoader

    @Bean
    fun chronicleConfiguration(): ChronicleConfiguration {
        return configurationLoader.logAndLoad("Chronicle Configuration", ChronicleConfiguration::class.java)
    }

    @Bean
    fun twilioConfiguration(): TwilioConfiguration {
        return configurationLoader.logAndLoad("Twilio Configuration", TwilioConfiguration::class.java)
    }

    @Bean
    fun mailConfiguration(): MailServiceConfig {
        return configurationLoader.logAndLoad("Mail configuration", MailServiceConfig::class.java)
    }

    @Bean
    fun storageResolver(): StorageResolver {
        return StorageResolver(dataSourceManager, chronicleConfiguration().storageConfiguration)
    }
}