package com.openlattice.chronicle.pods.tables

import com.openlattice.chronicle.storage.RedshiftDataTables
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.REDSHIFT_ENVIRONMENT
import com.geekbeast.postgres.PostgresTableDefinition
import com.geekbeast.postgres.PostgresTables
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.lang.reflect.Field
import java.lang.reflect.Modifier
import java.util.*
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
            Stream.of(*RedshiftDataTables::class.java.fields)
                    .filter { field: Field ->
                        (Modifier.isStatic(field.modifiers) && Modifier.isFinal(field.modifiers))
                    }.filter { field: Field ->
                        PostgresTableDefinition::class.java.isAssignableFrom(field.type)
                    }
                    .map { field: Field ->
                        try {
                            return@map field[null] as PostgresTableDefinition
                        } catch (e: IllegalAccessException) {
                            return@map null
                        }
                    }.filter { obj: PostgresTableDefinition? ->
                        Objects.nonNull(
                                obj
                        )
                    }
        }
    }
}
