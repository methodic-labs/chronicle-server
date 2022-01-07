package com.openlattice.chronicle.util

import com.openlattice.chronicle.authorization.AceKey
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.Principal
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
fun getLastAclKeySafely(aclKeys: List<UUID>): UUID? = aclKeys.lastOrNull()

fun toAceKeys(aclKeys: Set<AclKey>, principal: Principal): Set<AceKey> = aclKeys.mapTo(mutableSetOf()) {
    AceKey(it, principal)
}