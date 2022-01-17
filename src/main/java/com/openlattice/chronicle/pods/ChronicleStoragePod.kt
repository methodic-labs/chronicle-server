package com.openlattice.chronicle.pods

import com.geekbeast.rhizome.pods.ConfigurationLoader
import com.geekbeast.ResourceConfigurationLoader
import com.openlattice.chronicle.configuration.ChronicleConfiguration
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.jdbc.DataSourceManager
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
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