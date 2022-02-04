package com.openlattice.chronicle.services.download

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class ParticipantDataIterable(private val columnTitles: List<String>) : Iterable<Map<String, Set<Any>>> {
    override fun iterator(): Iterator<Map<String, Set<Any>>> {
        //Should probably use BasePostgresIterable if we keep this at all
        TODO("Not yet implemented")
    }

    fun getColumnTitles(): List<String> {
        TODO("Not yet implemented")
    }
}