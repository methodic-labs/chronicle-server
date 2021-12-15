package com.openlattice.chronicle.pods

import com.openlattice.chronicle.storage.PostgresDataTables
import com.openlattice.postgres.PostgresTables
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.util.stream.Stream

/**
 * When included as a pod this class automatically registers core openlattice tables for running the system.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Configuration
@Profile(PostgresDataTables.POSTGRES_DATA_ENVIRONMENT)
class PostgresTablesPod {
    @Bean
    fun postgresDataTables(): PostgresTables {
        return PostgresTables {
            Stream.of(
                    PostgresDataTables.CHRONICLE_USAGE_EVENTS,
                    PostgresDataTables.CHRONICLE_USAGE_STATS
            )
        }
    }
}
