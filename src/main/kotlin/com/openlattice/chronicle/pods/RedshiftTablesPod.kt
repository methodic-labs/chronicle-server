package com.openlattice.chronicle.pods

import com.openlattice.chronicle.storage.RedshiftDataTables
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.REDSHIFT_ENVIRONMENT
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
@Profile(REDSHIFT_ENVIRONMENT)
class RedshiftTablesPod {
    @Bean
    fun redshiftTables(): PostgresTables {
        return PostgresTables {
            Stream.of(
                    RedshiftDataTables.CHRONICLE_USAGE_EVENTS,
                    RedshiftDataTables.CHRONICLE_USAGE_STATS
            )
        }
    }
}
