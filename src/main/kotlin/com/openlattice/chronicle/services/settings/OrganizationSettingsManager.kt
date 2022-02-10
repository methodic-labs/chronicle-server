package com.openlattice.chronicle.services.settings

import com.openlattice.chronicle.organizations.OrganizationSettings
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface OrganizationSettingsManager {
    /**
     * This is a hack that shouldn't be used.
     */
    @Deprecated("This is a migration hack.")
    fun getOrganizationSettings( organizationId: UUID) : OrganizationSettings
}