package com.openlattice.chronicle.converters

import com.geekbeast.postgres.streams.BasePostgresIterable

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class TimeUseDiaryPostgresDownloadWrapper(
    iterable: BasePostgresIterable<List<Map<String, Any>>>
): Iterable<List<Map<String, Any>>> by iterable {

    companion object {
        private val DEFAULT_COLUMNS = listOf<String>()
    }

    var columnAdvice = DEFAULT_COLUMNS

    fun withColumnAdvice(columnAdvice: List<String>): TimeUseDiaryPostgresDownloadWrapper {
        this.columnAdvice = columnAdvice
        return this
    }
}
