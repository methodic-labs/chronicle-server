package com.openlattice.chronicle.organizations

import com.geekbeast.configuration.postgres.PostgresFlavor

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
fun ensureVanilla(flavor: PostgresFlavor) {
    check(flavor == PostgresFlavor.VANILLA) { "Only vanilla postgres supported for organizations." }
}