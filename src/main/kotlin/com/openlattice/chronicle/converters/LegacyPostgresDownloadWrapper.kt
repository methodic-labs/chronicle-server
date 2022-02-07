package com.openlattice.chronicle.converters

import com.geekbeast.postgres.streams.BasePostgresIterable
import java.sql.ResultSet

/**
 * Decorates a postgres iterable with a list of columns that can be used to generate a CSV download of data
 * directly from a postgres query. It is expected
 * @param iterable The base postgres iterable to wrap.
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class LegacyPostgresDownloadWrapper(
    iterable: Iterable<Map<String, Set<Any>>>
) :
    Iterable<Map<String, Set<Any>>> by iterable {
    companion object {
        private val DEFAULT_COLUMNS = listOf<String>()
        fun adapt(iterable: Iterable<Map<String, Any>>): Iterable<Map<String, Set<Any>>> {
            return iterable.asSequence().map { m -> m.mapValues { setOf(it.value) } }.asIterable()
        }

        fun adapt(iterable: PostgresDownloadWrapper): Iterable<Map<String, Set<Any>>> {
            return LegacyPostgresDownloadWrapper(
                iterable
                    .asSequence()
                    .map { m -> m.mapValues { setOf(it.value) } }
                    .asIterable())
                .withColumnAdvice(iterable.columnAdvice)
        }
    }

    /**
     * List of columns that are expected in the base postgres iterable. Should be the keys to each element returned
     * by the iterator
     */
    var columnAdvice = DEFAULT_COLUMNS

    fun withColumnAdvice(columnAdvice: List<String>): LegacyPostgresDownloadWrapper {
        this.columnAdvice = columnAdvice
        return this
    }
}