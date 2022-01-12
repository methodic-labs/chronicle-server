/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */
package com.openlattice.chronicle.authorization.principals

import com.google.common.base.MoreObjects
import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableSortedSet
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.openlattice.chronicle.authorization.Principal
import com.openlattice.chronicle.authorization.PrincipalType
import com.openlattice.chronicle.authorization.SecurablePrincipal
import com.openlattice.chronicle.authorization.SortedPrincipalSet
import com.openlattice.chronicle.hazelcast.HazelcastMap
import org.slf4j.LoggerFactory
import org.springframework.security.core.Authentication
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.core.context.SecurityContextHolder
import java.util.*
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import javax.annotation.Nonnull

object Principals {
    private val logger = LoggerFactory
        .getLogger(Principals::class.java)
    private val startupLock: Lock = ReentrantLock()
    private lateinit var securablePrincipals: IMap<String, SecurablePrincipal>
    private lateinit var principals: IMap<String, SortedPrincipalSet>
    fun init(spm: SecurePrincipalsManager, hazelcastInstance: HazelcastInstance) {
        if (startupLock.tryLock()) {
            securablePrincipals = HazelcastMap.SECURABLE_PRINCIPALS.getMap(
                hazelcastInstance!!
            )
            principals = HazelcastMap.RESOLVED_PRINCIPAL_TREES.getMap(
                hazelcastInstance
            )
        } else {
            logger.error("Principals security processing can only be initialized once.")
            throw IllegalStateException("Principals context already initialized.")
        }
    }

    fun ensureRole(principal: Principal) {
        Preconditions.checkArgument(
            principal.type == PrincipalType.ROLE,
            "Only role principal type allowed."
        )
    }

    fun ensureUser(principal: Principal) {
        Preconditions.checkState(principal.type == PrincipalType.USER, "Only user principal type allowed.")
    }

    fun ensureUserOrRole(principal: Principal) {
        Preconditions.checkState(
            principal.type == PrincipalType.USER || (principal.type
                    == PrincipalType.ROLE), "Only user and role principal types allowed."
        )
    }

    /**
     * This will retrieve the current user. If auth information isn't present an NPE is thrown (by design). If the wrong
     * type of auth is present a ClassCast exception will be thrown (by design).
     *
     * @return The principal for the current request.
     */
    @get:Nonnull
    val currentUser: Principal
        get() = getUserPrincipal(currentPrincipalId)
    val currentSecurablePrincipal: SecurablePrincipal
        get() = securablePrincipals[currentPrincipalId]!!

    fun getUserPrincipal(principalId: String): Principal {
        return Principal(PrincipalType.USER, principalId)
    }

    fun getUserPrincipals(principalId: String): NavigableSet<Principal> {
        return principals[principalId]!!
    }

    val currentPrincipals: NavigableSet<Principal>
        get() = MoreObjects.firstNonNull(
            principals!![currentPrincipalId], ImmutableSortedSet.of()
        )

    fun fromPrincipal(p: Principal): SimpleGrantedAuthority {
        return SimpleGrantedAuthority(p.type.name + "|" + p.id)
    }

    private fun getPrincipalId(authentication: Authentication): String {
        return authentication.principal.toString()
    }

    private val currentPrincipalId: String
        private get() = getPrincipalId(SecurityContextHolder.getContext().authentication)

    fun invalidatePrincipalCache(principalId: String) {
        securablePrincipals!!.evict(principalId)
        principals!!.evict(principalId)
    }
}