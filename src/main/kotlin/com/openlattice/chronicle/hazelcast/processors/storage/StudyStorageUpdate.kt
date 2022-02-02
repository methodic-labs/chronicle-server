package com.openlattice.chronicle.hazelcast.processors.storage

import com.geekbeast.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.chronicle.study.Study
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class StudyStorageUpdate(val storage: String) : AbstractRhizomeEntryProcessor<UUID, Study, Void?>() {
    override fun process(entry: MutableMap.MutableEntry<UUID, Study?>): Void? {
        entry.value?.storage = storage
        return null
    }
}