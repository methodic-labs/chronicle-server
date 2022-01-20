package com.openlattice.chronicle.authorization.processors

import com.hazelcast.core.Offloadable
import com.openlattice.chronicle.authorization.AceKey
import com.openlattice.chronicle.authorization.AceValue
import com.openlattice.chronicle.authorization.DelegatedPermissionEnumSet
import com.openlattice.chronicle.authorization.Permission
import com.geekbeast.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import java.util.*

class AuthorizationEntryProcessor : AbstractReadOnlyRhizomeEntryProcessor<AceKey, AceValue, DelegatedPermissionEnumSet>(), Offloadable {

    override fun process(entry: MutableMap.MutableEntry<AceKey, AceValue?>): DelegatedPermissionEnumSet {
        return DelegatedPermissionEnumSet.wrap(entry.value?.permissions ?: EnumSet.noneOf(Permission::class.java))
    }

    override fun getExecutorName(): String {
        return Offloadable.OFFLOADABLE_EXECUTOR
    }
}