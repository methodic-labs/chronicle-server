package com.openlattice.chronicle.providers

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface LateInitAware {
    fun setLateInitProvider(lateInitProvider: LateInitProvider)
}