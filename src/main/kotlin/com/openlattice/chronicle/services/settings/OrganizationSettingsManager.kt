package com.openlattice.chronicle.services.settings

import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface OrganizationSettingsManager {
    fun getOrganizationSettings( organizationId: UUID) : OrganizationSettings
}