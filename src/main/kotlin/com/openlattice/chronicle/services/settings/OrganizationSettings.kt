package com.openlattice.chronicle.services.settings

import com.openlattice.chronicle.constants.AppUsageFrequency

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class ChronicleDataCollectionSettings(
        val appUsageFrequency: AppUsageFrequency = AppUsageFrequency.DAILY
)

data class OrganizationSettings(
        val chronicleDataCollection: ChronicleDataCollectionSettings
)