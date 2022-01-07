package com.openlattice.chronicle.authorization

import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class DelegatedPermissionEnumSet( val permissions: EnumSet<Permission>) : Set<Permission> by permissions {
    companion object {
        @JvmStatic
        fun wrap(permissions: EnumSet<Permission>): DelegatedPermissionEnumSet {
            return DelegatedPermissionEnumSet(permissions)
        }

        @JvmStatic
        fun wrap(permissions: Set<Permission>): DelegatedPermissionEnumSet {
            return DelegatedPermissionEnumSet(EnumSet.copyOf(permissions))
        }

    }

    fun unwrap(): EnumSet<Permission> {
        return permissions
    }

    /**
     * Auto generated equals does the wrong thing here and thinks that other EnumSet classes are not instances of set
     * or vice versa.
     */
    override fun equals(other: Any?): Boolean {
        return permissions == other
    }
}