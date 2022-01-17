/*
 * Copyright (C) 2018. OpenLattice, Inc.
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
package com.openlattice.chronicle.authorization.principals.processors

import com.geekbeast.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.SecurablePrincipal

data class PrincipalTitleUpdater(val title: String) : AbstractRhizomeEntryProcessor<AclKey, SecurablePrincipal, Any?>() {
    override fun process(entry: MutableMap.MutableEntry<AclKey, SecurablePrincipal?>): Any? {
        val principal = entry.value
        if (principal != null) {
            principal.title = title!!
            //Need to let Hazelcast know to persist title update
            entry.setValue(principal)
        }
        return null
    }

    companion object {
        private const val serialVersionUID = -717197511031518227L
    }
}