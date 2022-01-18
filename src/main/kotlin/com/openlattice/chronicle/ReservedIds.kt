package com.openlattice.chronicle

import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

enum class ReservedIds(val id: UUID) {
    SYSTEM(UUID(0, 0)),
    STUDY(UUID(0, 0))
}