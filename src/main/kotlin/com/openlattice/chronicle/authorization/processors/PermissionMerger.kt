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
package com.openlattice.chronicle.authorization.processors

import com.hazelcast.core.Offloadable
import com.geekbeast.rhizome.hazelcast.processors.AbstractMerger
import com.openlattice.chronicle.authorization.AceKey
import com.openlattice.chronicle.authorization.AceValue
import com.openlattice.chronicle.authorization.Permission
import com.openlattice.chronicle.authorization.SecurableObjectType
import java.time.OffsetDateTime
import java.util.*

data class PermissionMerger( private val objects: Iterable<Permission>,
                                    val securableObjectType: SecurableObjectType,
                                    val expirationDate: OffsetDateTime = OffsetDateTime.MAX) : AbstractMerger<AceKey, AceValue, Permission>(objects), Offloadable {
    companion object {
        private const val serialVersionUID = -3504613417625318717L
    }
    override fun processBeforeWriteBack(value: AceValue) {
        value.securableObjectType = securableObjectType
        value.expirationDate = expirationDate
    }

    override fun newEmptyCollection(): AceValue {
        return AceValue(EnumSet.noneOf(Permission::class.java), securableObjectType, expirationDate)
    }

    override fun getExecutorName(): String = Offloadable.OFFLOADABLE_EXECUTOR
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        if (!super.equals(other)) return false

        other as PermissionMerger

        if (objects != other.objects) return false
        if (securableObjectType != other.securableObjectType) return false
        if (expirationDate != other.expirationDate) return false

        return true
    }

    override fun hashCode(): Int {
        var result = super.hashCode()
        result = 31 * result + objects.hashCode()
        result = 31 * result + securableObjectType.hashCode()
        result = 31 * result + expirationDate.hashCode()
        return result
    }

}