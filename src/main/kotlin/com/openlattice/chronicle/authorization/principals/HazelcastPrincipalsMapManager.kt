package com.openlattice.chronicle.authorization.principals

import com.google.common.base.Preconditions
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicates
import com.openlattice.chronicle.authorization.*
import com.openlattice.chronicle.authorization.reservations.AclKeyReservationService
import com.openlattice.chronicle.authorization.principals.PrincipalsMapManager.Companion.findPrincipal
import com.openlattice.chronicle.authorization.principals.PrincipalsMapManager.Companion.getFirstSecurablePrincipal
import com.openlattice.chronicle.authorization.principals.PrincipalsMapManager.Companion.hasPrincipalType
import com.openlattice.chronicle.hazelcast.HazelcastMap

/**
 * Principals manager manages the principals map
 *
 * @author Drew Bailey (drew@openlattice.com)
 */
class HazelcastPrincipalsMapManager(
    hazelcastInstance: HazelcastInstance,
    private val reservations: AclKeyReservationService
) : PrincipalsMapManager {

    private val principals = HazelcastMap.PRINCIPALS.getMap(hazelcastInstance)

    override fun lookupRole(aclKey: AclKey): Role {
        val principal = principals.getValue(aclKey)
        return castSecurablePrincipalAsRole(principal)
    }

    override fun lookupRole(principal: Principal): Role {
        val sp = getFirstSecurablePrincipal(principals, findPrincipal(principal))
        return castSecurablePrincipalAsRole(sp)
    }

    override fun getAllRoles(): Set<Role> {
        return principals.values(hasPrincipalType(PrincipalType.ROLE)).map { it as Role }.toSet()
    }

    override fun getSecurablePrincipal(principalId: String): SecurablePrincipal {
        val id = Preconditions.checkNotNull(
            reservations.getId(HazelcastPrincipalService.getPrincipalReservationName(principalId)),
            "AclKey not found for Principal %s", principalId
        )
        return principals.values(
            Predicates.equal(PrincipalMapstore.PRINCIPAL_ID_INDEX, id)
        ).first()
    }

    override fun getSecurablePrincipal(aclKey: AclKey): SecurablePrincipal? {
        return principals[aclKey]
    }

    override fun getSecurablePrincipal(principal: Principal): SecurablePrincipal {
        return getFirstSecurablePrincipal(principals, findPrincipal(principal))
    }

    override fun getSecurablePrincipals(aclKeys: Set<AclKey>): Map<AclKey, SecurablePrincipal> {
        return principals.getAll(aclKeys)
    }

    override fun getAclKeyByPrincipal(ps: Set<Principal>): Map<Principal, AclKey> {
        return principals.values(Predicates.`in`(PrincipalMapstore.PRINCIPAL_INDEX, *ps.toTypedArray())).associate {
            it.principal to it.aclKey
        }
    }

    private fun castSecurablePrincipalAsRole(sp: SecurablePrincipal): Role {
        require(sp.principal.type == PrincipalType.ROLE) { "The provided principal $sp is not a role" }
        return sp as Role
    }
}