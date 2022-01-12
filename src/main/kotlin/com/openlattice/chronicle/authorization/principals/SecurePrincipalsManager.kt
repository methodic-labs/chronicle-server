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

import com.auth0.json.mgmt.users.User
import com.hazelcast.query.Predicate
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.Principal
import com.openlattice.chronicle.authorization.Role
import com.openlattice.chronicle.authorization.SecurablePrincipal
import java.util.UUID
import javax.annotation.Nonnull

interface SecurePrincipalsManager {
    /**
     * @param owner     The owner of a role. Usually the organization.
     * @param principal The principal which to create.
     * @return True if the securable principal was created false otherwise.
     */
    fun createSecurablePrincipalIfNotExists(owner: Principal, principal: SecurablePrincipal): Boolean

    /**
     * Retrieves a securable principal by acl key lookup.
     *
     * @param aclKey The acl key for the securable principal.
     * @return The securable principal identified by acl key.
     */
    fun getSecurablePrincipal(aclKey: AclKey): SecurablePrincipal
    fun getAllRolesInOrganization(organizationId: UUID): Collection<SecurablePrincipal>
    fun getAllRolesInOrganizations(organizationIds: Collection<UUID>): Map<UUID, Collection<SecurablePrincipal>>
    fun getSecurablePrincipals(p: Predicate<AclKey, SecurablePrincipal>): Collection<SecurablePrincipal>
    fun updateTitle(aclKey: AclKey, title: String)
    fun updateDescription(aclKey: AclKey, description: String)
    fun deletePrincipal(aclKey: AclKey)
    fun deleteAllRolesInOrganization(organizationId: UUID)
    fun addPrincipalToPrincipal(source: AclKey, target: AclKey)

    /**
     * Grants an AclKey to a set of AclKeys, and returns any that were updated.
     *
     * @param source  The child AclKey to grant
     * @param targets The parent AclKeys that will be granted [source]
     * @return all AclKeys that were updated. Any target AclKey that already had [source] as a child will not be included.
     */
    fun addPrincipalToPrincipals(source: AclKey, targets: Set<AclKey>): Set<AclKey>
    fun removePrincipalFromPrincipal(source: AclKey, target: AclKey)
    fun removePrincipalsFromPrincipals(sources: Set<AclKey>, target: Set<AclKey>)

    /**
     * Reads
     */
    @Nonnull
    fun getSecurablePrincipal(principalId: String): SecurablePrincipal
    fun getSecurablePrincipals(aclKeys: Set<AclKey>): Map<AclKey, SecurablePrincipal>
    fun getParentPrincipalsOfPrincipal(aclKey: AclKey): Collection<SecurablePrincipal>
    fun getOrganizationMembers(organizationIds: Set<UUID>): Map<UUID, Set<SecurablePrincipal>>
    fun getOrganizationMemberPrincipals(organizationId: UUID): Set<Principal>
    fun principalHasChildPrincipal(parent: AclKey, child: AclKey): Boolean

    // Methods about users
    fun getAllUserProfilesWithPrincipal(principal: AclKey): Collection<User>
    fun principalExists(p: Principal): Boolean
    fun getUser(userId: String): User
    fun getRole(organizationId: UUID, roleId: UUID): Role
    fun lookup(p: Principal): AclKey
    fun lookup(principals: Set<Principal>): Map<Principal, AclKey>
    fun lookupRole(principal: Principal): Role
    fun getSecurablePrincipals(members: Collection<Principal>): Collection<SecurablePrincipal>
    fun getAllPrincipals(sp: SecurablePrincipal): Collection<SecurablePrincipal>
    fun bulkGetUnderlyingPrincipals(sps: Set<SecurablePrincipal>): Map<SecurablePrincipal, Set<Principal>>
    val currentUserId: UUID
    fun ensurePrincipalsExist(aclKeys: Set<AclKey>)
    val allRoles: Set<Role>
    val allUsers: Set<SecurablePrincipal>
}