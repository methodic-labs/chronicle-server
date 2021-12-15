package com.openlattice.chronicle.services.settings

import com.openlattice.chronicle.constants.AppComponent
import com.openlattice.chronicle.constants.AppUsageFrequency
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class ChronicleDataCollectionSettings(
        val appUsageFrequency: AppUsageFrequency = AppUsageFrequency.DAILY
)

data class OrganizationSettings(
        val chronicleDataCollection: ChronicleDataCollectionSettings,
        val appSettings: Map<AppComponent,Map<String,Any>> = mutableMapOf()
)