package com.openlattice.chronicle.services.android

import com.hazelcast.core.HazelcastInstance
import com.openlattice.chronicle.hazelcast.HazelcastMap
import org.springframework.stereotype.Service

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
@Service
class AndroidApplicationLabelResolutionService(hazelcast: HazelcastInstance) : AndroidApplicationLabelResolutionManager {
    private val androidApps = HazelcastMap.ANDROID_APPS.getMap(hazelcast)

    /**
     * Given an android app package name, attempts to resolve to a human-friendly label.
     * This method looks up the package name in the cache, and if found, returns the mapped value.
     * If not found, a new <package, label> entry is inserted into the cache only if the entry is valid
     * An entry is considered valid if package name is different from label
     */
    override fun resolve(appPackageName: String, maybeApplicationLabel: String): String {
        // look up package name in mapstore
        val result = androidApps[appPackageName]
        result?.let { return it }

        // Only update mapstore if entry is valid
        if (appPackageName != maybeApplicationLabel) {
            androidApps.put(appPackageName, maybeApplicationLabel)
        }

        return maybeApplicationLabel
    }
}