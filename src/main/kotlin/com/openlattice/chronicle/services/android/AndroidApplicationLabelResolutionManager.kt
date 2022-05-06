package com.openlattice.chronicle.services.android

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
interface AndroidApplicationLabelResolutionManager {
    fun resolve(appPackageName: String, maybeApplicationLabel: String): String
}