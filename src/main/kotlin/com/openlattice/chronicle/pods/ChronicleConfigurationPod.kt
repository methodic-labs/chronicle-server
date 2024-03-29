package com.openlattice.chronicle.pods

import com.geekbeast.hazelcast.PreHazelcastUpgradeService
import com.geekbeast.jdbc.DataSourceManager
import com.geekbeast.mail.MailServiceConfig
import com.geekbeast.rhizome.pods.ConfigurationLoader
import com.openlattice.chronicle.configuration.ChronicleConfiguration
import com.openlattice.chronicle.configuration.TwilioConfiguration
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.upgrades.*
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

    @Bean
    fun upgradeService(): UpgradeService {
        return UpgradeService(storageResolver())
    }

    @Bean
    fun studyLimitsUpgrade(): PreHazelcastUpgradeService {
        return StudyLimitsUpgrade(storageResolver(), upgradeService())
    }

    @Bean
    fun appFilteringUpgrade(): PreHazelcastUpgradeService {
        return AppFilteringUpgrade(storageResolver(), upgradeService())
    }

    @Bean
    fun studySettingsUpgrade(): PreHazelcastUpgradeService {
        return StudySettingsUpgrade(storageResolver(), upgradeService())
    }

    @Bean
    fun uploadAtUpgrade(): PreHazelcastUpgradeService {
        return UploadAtUpgrade(storageResolver(), upgradeService())
    }

    @Bean
    fun participantStatsUpgrade(): PreHazelcastUpgradeService {
        return ParticipantStatsUpgrade(storageResolver(), upgradeService())
    }
}