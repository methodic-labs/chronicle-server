package com.openlattice.chronicle.pods

import com.geekbeast.rhizome.pods.ConfigurationLoader
import com.openlattice.chronicle.configuration.ChronicleConfiguration
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
    private lateinit var configurationLoader: ConfigurationLoader

    @Bean
    fun chronicleConfiguration(): ChronicleConfiguration {
        return configurationLoader.logAndLoad("Chronicle Configuration", ChronicleConfiguration::class.java)
    }
}