package com.openlattice.chronicle.services.surveys

import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@getmethodic.com&gt;
 */
data class IosDeviceUsageByCategory(
    val bundleIdentifier: String?,
    val category: String,
    val usageInSeconds: Double,
)