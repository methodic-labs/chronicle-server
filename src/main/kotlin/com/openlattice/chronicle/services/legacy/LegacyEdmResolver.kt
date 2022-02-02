package com.openlattice.chronicle.services.legacy

import com.openlattice.chronicle.constants.EdmConstants
import com.openlattice.chronicle.storage.RedshiftColumns
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
            propertyTypes[EdmConstants.RRULE_FQN] = UUID.fromString("2d7e9eaf-8404-42b6-ba98-4287eab4901d")
            propertyTypes[EdmConstants.DATE_LOGGED_FQN] = UUID.fromString("e90a306c-ee37-4cd1-8a0e-71ad5a180340")
            propertyTypes[EdmConstants.STRING_ID_FQN] = UUID.fromString("ee3a7573-aa70-4afb-814d-3fad27cda988")
            propertyTypes[EdmConstants.FULL_NAME_FQN] = UUID.fromString("70d2ff1c-2450-4a47-a954-a7641b7399ae")
            propertyTypes[EdmConstants.RECORD_TYPE_FQN] = UUID.fromString("285e6bfc-2a73-49ae-8cb2-b112244ed85d")
            propertyTypes[EdmConstants.TIMEZONE_FQN] = UUID.fromString("071ba832-035f-4b04-99e4-d11dc4fbe0e8")
            propertyTypes[EdmConstants.USER_FQN] = UUID.fromString("188b754c-bd92-4f4a-8d01-a57fe94adc6d")
            propertyTypes[EdmConstants.TITLE_FQN] = UUID.fromString("f0373614-c607-43b2-99b0-1cd32ff4f921")
            propertyTypes[EdmConstants.START_DATE_TIME_FQN] = UUID.fromString("92a6a5c5-b4f1-40ce-ace9-be232acdce2a")
            propertyTypes[EdmConstants.END_DATE_TIME_FQN] = UUID.fromString("0ee3acba-51a7-4f8d-921f-e23d75b07f65")
            propertyTypes[EdmConstants.DURATION_FQN] = UUID.fromString("c106ee75-f18e-48ed-bc85-b75702bfe802")
        }

        @JvmStatic
        fun getPropertyTypeId(fqn: FullQualifiedName): UUID = propertyTypes.getValue(fqn)

        @JvmStatic
        fun getPropertyTypeIds(
                fqns: Collection<FullQualifiedName>
        ): Map<FullQualifiedName, UUID> = fqns.associateWith { propertyTypes.getValue(it) }

        /**
         *
         */
        fun getLegacyPropertyTypeIds(fqns: Collection<String>): Map<String, UUID> {
            return fqns.associateWith { propertyTypes.getValue(FullQualifiedName(it)) }
        }
    }
}