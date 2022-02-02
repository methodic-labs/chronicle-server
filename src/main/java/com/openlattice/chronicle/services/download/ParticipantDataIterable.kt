package com.openlattice.chronicle.services.download

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class ParticipantDataIterable(
    private val columnTitles: List<String>,
    private val data: Iterable<Map<String,Set<Any>>>
) : Iterable<Map<String, Set<Any>>> {
    override fun iterator(): Iterator<Map<String, Set<Any>>> {
        //Should probably use BasePostgresIterable if we keep this at all
        return data.iterator()
    }

    fun getColumnTitles(): List<String> {
        return columnTitles
    }
}