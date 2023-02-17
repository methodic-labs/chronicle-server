package com.openlattice.chronicle.storage

import java.time.OffsetDateTime

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
fun odtFromUsageEventColumn(value: Any?): OffsetDateTime? {
    if (value == null) return null
    return when (value) {
        is String -> OffsetDateTime.parse(value)
        is OffsetDateTime -> value
        else -> throw UnsupportedOperationException("${value.javaClass.canonicalName} is not a supported date time class.")
    }
}