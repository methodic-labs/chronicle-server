package com.openlattice.chronicle.services.legacy

import com.openlattice.chronicle.constants.EdmConstants
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class LegacyEdmResolver() {
    companion object {
        private val propertyTypes = mutableMapOf<FullQualifiedName, UUID>()

        init {
            propertyTypes[EdmConstants.DATE_LOGGED_FQN] = UUID.fromString("e90a306c-ee37-4cd1-8a0e-71ad5a180340")
        }

        @JvmStatic
        fun getPropertyTypeId(fqn: FullQualifiedName): UUID = propertyTypes.getValue(fqn)

        @JvmStatic
        fun getPropertyTypeIds(
                fqns: Collection<FullQualifiedName>
        ): Map<FullQualifiedName, UUID> = fqns.associateWith { propertyTypes.getValue(it) }
    }
}