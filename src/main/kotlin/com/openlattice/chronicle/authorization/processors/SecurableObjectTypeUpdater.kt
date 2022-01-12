/*
 * Copyright (C) 2017. OpenLattice, Inc
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */
package com.openlattice.chronicle.authorization.processors

import com.hazelcast.core.Offloadable
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.chronicle.authorization.AceKey
import com.openlattice.chronicle.authorization.AceValue
import com.openlattice.chronicle.authorization.SecurableObjectType

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class SecurableObjectTypeUpdater(val securableObjectType: SecurableObjectType) :
    AbstractRhizomeEntryProcessor<AceKey, AceValue, Void?>(), Offloadable {

    override fun process(entry: MutableMap.MutableEntry<AceKey, AceValue?>): Void? {
        val value = entry.value
        if (value != null) {
            value.securableObjectType = securableObjectType
            entry.setValue(value)
        }
        return null
    }

    override fun getExecutorName(): String {
        return Offloadable.OFFLOADABLE_EXECUTOR
    }
}