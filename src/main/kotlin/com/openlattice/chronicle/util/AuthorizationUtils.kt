package com.openlattice.chronicle.util

import com.geekbeast.configuration.postgres.PostgresFlavor
import com.openlattice.chronicle.authorization.AceKey
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.Principal
import java.util.UUID

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

fun ensureVanilla(flavor: PostgresFlavor, message: () -> String = { "only vanilla postgres is supported" }) {
    check(flavor == PostgresFlavor.VANILLA) { message }
}

fun getLastAclKeySafely(aclKeys: List<UUID>): UUID? = aclKeys.lastOrNull()

fun toAceKeys(aclKeys: Set<AclKey>, principal: Principal): Set<AceKey> = aclKeys.mapTo(mutableSetOf()) {
    AceKey(it, principal)
}
