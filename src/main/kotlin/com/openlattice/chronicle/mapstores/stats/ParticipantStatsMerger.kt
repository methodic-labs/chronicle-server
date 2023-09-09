package com.openlattice.chronicle.mapstores.stats

import com.geekbeast.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.chronicle.participants.ParticipantStats

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@getmethodic.com&gt;
 */
class ParticipantStatsMerger(val participantStats: ParticipantStats) :
    AbstractRhizomeEntryProcessor<ParticipantKey, ParticipantStats, Void?>() {
    override fun process(entry: MutableMap.MutableEntry<ParticipantKey, ParticipantStats>): Void? {
        participantStats.

        return null
    }
}