package com.openlattice.chronicle.services.legacy

import com.openlattice.chronicle.organizations.ChronicleDataCollectionSettings
import com.openlattice.chronicle.organizations.OrganizationSettings

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class LegacyUtil {
    companion object {
        @JvmStatic
        fun mapToLegacySettings(organizationSettings: OrganizationSettings): Map<String, Any> {
            TODO("Implement legacy settings mapping")
        }

        @JvmStatic
        fun mapToLegacySettings(chronicleDataCollectionSettings: ChronicleDataCollectionSettings): Map<String, Any> {
            TODO("Implement legacy settings mapping")
        }
    }
}