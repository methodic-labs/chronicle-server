package com.openlattice.chronicle.mapstores.stats

import com.geekbeast.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.google.common.base.MoreObjects
import com.openlattice.chronicle.participants.ParticipantStats

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@getmethodic.com&gt;
 */
class ParticipantStatsMerger(val statsToMerge: ParticipantStats) :
    AbstractRhizomeEntryProcessor<ParticipantKey, ParticipantStats, Void?>() {
    override fun process(entry: MutableMap.MutableEntry<ParticipantKey, ParticipantStats?>): Void? {
        val current = entry.value

        if (current == null) {
            entry.setValue(statsToMerge)
        } else {
            entry.setValue(
                ParticipantStats(
                    studyId = current.studyId,
                    participantId = current.participantId,
                    androidLastPing = maxOrFirstNotNull(current.androidLastPing, statsToMerge.androidLastPing),
                    androidFirstDate = minOrFirstNotNull(current.androidFirstDate, statsToMerge.androidFirstDate),
                    androidLastDate = maxOrFirstNotNull(current.androidLastDate, statsToMerge.androidLastDate),
                    androidUniqueDates = current.androidUniqueDates + statsToMerge.androidUniqueDates,
                    iosLastPing = maxOrFirstNotNull(current.iosLastPing, statsToMerge.iosLastPing),
                    iosFirstDate = minOrFirstNotNull(current.iosFirstDate, statsToMerge.iosFirstDate),
                    iosLastDate = maxOrFirstNotNull(current.iosLastDate, statsToMerge.iosLastDate),
                    iosUniqueDates = current.iosUniqueDates + statsToMerge.iosUniqueDates,
                    tudFirstDate = minOrFirstNotNull(current.tudFirstDate, statsToMerge.tudFirstDate),
                    tudLastDate = maxOrFirstNotNull(current.tudLastDate, statsToMerge.tudLastDate),
                    tudUniqueDates = current.tudUniqueDates + statsToMerge.tudUniqueDates
                )
            )
        }

        return null
    }

    private fun <T : Comparable<T>> minOrFirstNotNull(a: T?, b: T?): T? {
        return if (a == null && b != null) {
            b
        } else if (b == null && a != null) {
            a
        } else if (a != null && b != null) {
            minOf(a, b)
        } else {
            return null
        }
    }

    private fun <T : Comparable<T>> maxOrFirstNotNull(a: T?, b: T?): T? {
        return if (a == null && b != null) {
            b
        } else if (b == null && a != null) {
            a
        } else if (a != null && b != null) {
            maxOf(a, b)
        } else {
            return null
        }
    }
}