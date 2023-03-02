package com.openlattice.chronicle.storage.healthchecks

import com.codahale.metrics.health.HealthCheck
import com.openlattice.chronicle.storage.StorageResolver

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class RedshiftHealthcheck(private val storageResolver: StorageResolver) : HealthCheck {

    override fun check(): Result {
        storageResolver.
    }
}