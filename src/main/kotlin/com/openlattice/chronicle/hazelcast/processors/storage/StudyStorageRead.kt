package com.openlattice.chronicle.hazelcast.processors.storage

import com.geekbeast.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import com.hazelcast.core.Offloadable
import com.openlattice.chronicle.study.Study
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class StudyStorageRead : AbstractReadOnlyRhizomeEntryProcessor<UUID, Study, String?>(), Offloadable {
    override fun getExecutorName(): String = Offloadable.OFFLOADABLE_EXECUTOR
    override fun process(entry: MutableMap.MutableEntry<UUID, Study?>): String? {
        return entry.value?.storage
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        return true
    }

    override fun hashCode(): Int {
        return javaClass.hashCode()
    }

}