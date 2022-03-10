package com.openlattice.chronicle.services.studies.processors

import com.geekbeast.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import com.openlattice.chronicle.study.Study
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class StudyPhoneNumberGetter : AbstractReadOnlyRhizomeEntryProcessor<UUID, Study, String?>() {
    override fun process(entry: MutableMap.MutableEntry<UUID, Study?>): String? {
        return entry.value?.phoneNumber
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