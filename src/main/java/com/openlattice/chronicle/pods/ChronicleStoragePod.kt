package com.openlattice.chronicle.pods

import com.kryptnostic.rhizome.configuration.ConfigurationConstants
import com.kryptnostic.rhizome.configuration.service.ConfigurationService
import com.kryptnostic.rhizome.pods.ConfigurationLoader
import com.openlattice.ResourceConfigurationLoader
import com.openlattice.chronicle.configuration.ChronicleConfiguration
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.jdbc.DataSourceManager
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.io.IOException
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Configuration
class ChronicleStoragePod {
    @Inject
    private lateinit var configurationLoader: ConfigurationLoader

    @Inject
    private lateinit var dataSourceManager: DataSourceManager

    @Throws(IOException::class)
    @Bean
    fun chronicleConfiguration(): ChronicleConfiguration {
        return ResourceConfigurationLoader.loadConfiguration(ChronicleConfiguration::class.java)
    }

    @Bean
    fun storageResolver(): StorageResolver {
        return StorageResolver(dataSourceManager, chronicleConfiguration().storageConfiguration)
    }
}