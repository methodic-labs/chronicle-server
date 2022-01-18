package com.openlattice.chronicle.organizations

import com.openlattice.chronicle.authorization.Principal
import com.openlattice.chronicle.storage.StorageResolver
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class   ChronicleOrganizationService(val storageResolver: StorageResolver) {
    fun createOrganization(organizationPrincipal: OrganizationPrincipal): UUID {

        TODO("Not yet implemented")
    }

    fun maybeGetOrganization( principal: Principal) : Optional<Organization> {
        TODO("Not yet implemented")
    }
}