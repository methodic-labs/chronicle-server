package com.openlattice.chronicle.pods.tables

import com.openlattice.chronicle.storage.ChroniclePostgresTables
import com.geekbeast.postgres.PostgresTableDefinition
import com.geekbeast.postgres.PostgresTables
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.lang.reflect.Field
import java.lang.reflect.Modifier
import java.util.*
import java.util.stream.Stream
import kotlin.streams.asStream

/**
 * When included as a pod this class automatically registers core openlattice tables for running the system.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Configuration
class PostgresTablesPod {
    @Bean
    fun postgresTables(): PostgresTables {
        return PostgresTables {
            (ChroniclePostgresTables::class.java.fields.asSequence() + ChroniclePostgresTables::class.java.declaredFields.asSequence())
                .filter { field: Field -> (Modifier.isStatic(field.modifiers) && Modifier.isFinal(field.modifiers)) }
                .filter { field: Field -> PostgresTableDefinition::class.java.isAssignableFrom(field.type) }
                .mapNotNull { field: Field ->
                    try {
                        field[null] as PostgresTableDefinition
                    } catch (e: IllegalAccessException) {
                        null
                    }
                }.asStream()
        }
    }
}
