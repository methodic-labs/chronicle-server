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
package com.openlattice.chronicle.authorization

import com.codahale.metrics.annotation.Timed
import com.google.common.collect.SetMultimap
import com.hazelcast.query.Predicate
import java.sql.Connection
import java.time.OffsetDateTime
import java.util.EnumSet
import java.util.EnumMap
import java.util.stream.Stream

/**
 * The authorization manager manages permissions for all securable objects in the system.
 *
 *
 * Authorization behavior is summarized below:
 *
 *  * No inheritance and that all permissions are explicitly set.
 *  * For permissions that are present we follow a least restrictive model for determining access
 *  * If no relevant permissions are present for Principal set, access is denied.
 *
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface AuthorizationManager {
    /**
     * Creates a securable object, registers it's type, and ensures that the creating principal at least has at least
     * owner permissions so they can manage the ACL via the API. This version of the API is designed to be used in
     * transactions and requires the caller to ensure that in memory map is refreshed after the transactions commits
     * as the mapstore cannot read uncommited changes.
     *
     * NOTE: There is still a failure mode here if the principal is a role that is not assigned to anyone.
     *
     * @param connection The SQL connection to be used for ensuring the creation of object happens transactionally
     * along with any other required operations.
     * @param aclKey The unique acl key for the object.
     * @param principal The creating principal.
     * @param permissions The permissions to grant to that principal.
     */
    @Timed
    fun createUnnamedSecurableObject(
        connection: Connection,
        aclKey: AclKey,
        principal: Principal,
        permissions: EnumSet<Permission> = EnumSet.allOf(Permission::class.java),
        objectType: SecurableObjectType,
        expirationDate: OffsetDateTime =  OffsetDateTime.MAX
    )

    /**
     * Creates a securable object, registers it's type, and ensures that the creating principal at least has at least
     * owner permissions so they can manage the ACL via the API. This will take care of updating the cache once the
     * object is inserted into the database.
     *
     * NOTE: There is still a failure mode here if the principal is a role that is not assigned to anyone.
     *
     * @param aclKey The unique acl key for the object.
     * @param principal The creating principal.
     * @param permissions The permissions to grant to that principal.
     */
    @Timed
    fun createUnnamedSecurableObject(
        aclKey: AclKey,
        principal: Principal,
        permissions: EnumSet<Permission> = EnumSet.allOf(Permission::class.java),
        objectType: SecurableObjectType,
        expirationDate: OffsetDateTime =  OffsetDateTime.MAX
    )

     @Timed
    fun addPermission(
        aclKeys: AclKey,
        principal: Principal,
        permissions: EnumSet<Permission>
    )

    @Timed
    fun addPermission(
        aclKeys: AclKey,
        principal: Principal,
        permissions: Set<Permission>,
        expirationDate: OffsetDateTime
    )

    @Timed
    fun addPermission(
        aclKeys: AclKey,
        principal: Principal,
        permissions: Set<Permission>,
        securableObjectType: SecurableObjectType,
        expirationDate: OffsetDateTime
    )

    /**
     * Method for bulk adding permissions to a single principal across multiple acl keys of the same type.
     *
     * @param keys                The acl keys to which permissions will be added.
     * @param principal           The principal who will be receiving permissions.
     * @param permissions         The permissions that will be added.
     * @param securableObjectType The securable object type for which the permissions are being added. This will
     * override the existing object type, so care must be taken to call this for keys of the right type.
     */
    @Timed
    fun addPermissions(
        keys: Set<AclKey>,
        principal: Principal,
        permissions: EnumSet<Permission>,
        securableObjectType: SecurableObjectType
    )

    /**
     * Method for bulk adding permissions to a single principal across multiple acl keys of the same type.
     *
     * @param keys                The acl keys to which permissions will be added.
     * @param principal           The principal who will be receiving permissions.
     * @param permissions         The permissions that will be added.
     * @param securableObjectType The securable object type for which the permissions are being added. This will
     * override the existing object type, so care must be taken to call this for keys of the right type.
     * @param expirationDate      The expiration data for the permission changes.
     */
    @Timed
    fun addPermissions(
        keys: Set<AclKey>,
        principal: Principal,
        permissions: EnumSet<Permission>,
        securableObjectType: SecurableObjectType,
        expirationDate: OffsetDateTime
    )

    @Timed
    fun addPermissions(acls: List<Acl>)

    @Timed
    fun removePermissions(acls: List<Acl>)

    @Timed
    fun setPermissions(acls: List<Acl>)

    @Timed
    fun removePermission(
        aclKeys: AclKey,
        principal: Principal,
        permissions: EnumSet<Permission>
    )

    @Timed
    fun setPermission(
        aclKeys: AclKey,
        principal: Principal,
        permissions: EnumSet<Permission>
    )

    @Timed
    fun setPermission(
        aclKeys: AclKey,
        principal: Principal,
        permissions: EnumSet<Permission>,
        expirationDate: OffsetDateTime
    )

    @Timed
    fun setPermission(aclKeys: Set<AclKey>, principals: Set<Principal>, permissions: EnumSet<Permission>)

    @Timed
    fun setPermissions(permissions: Map<AceKey, EnumSet<Permission>>)

    @Timed
    fun deletePermissions(aclKey: AclKey)

    @Timed
    fun deletePrincipalPermissions(principal: Principal)

    @Timed
    fun authorize(
        requests: Map<AclKey, EnumSet<Permission>>,
        principals: Set<Principal>
    ): Map<AclKey, EnumMap<Permission, Boolean>>

    @Timed
    fun accessChecksForPrincipals(
        accessChecks: Set<AccessCheck>,
        principals: Set<Principal>
    ): List<Authorization>

    @Timed
    fun checkIfHasPermissions(
        aclKeys: AclKey,
        principals: Set<Principal>,
        requiredPermissions: EnumSet<Permission>
    ): Boolean
    // Utility functions for retrieving permissions
    /**
     * @param aclKeySets the list of groups of AclKeys for wich to get the most restricted set of permissions
     * @param principals the pricipals to check against
     * @return the intersection of permission for each set of aclKeys
     */
    fun getSecurableObjectSetsPermissions(
        aclKeySets: Collection<Set<AclKey>>,
        principals: Set<Principal>
    ): Map<Set<AclKey>, EnumSet<Permission>>

    fun getSecurableObjectPermissions(
        aclKey: AclKey,
        principals: Set<Principal>
    ): Set<Permission>

    fun getAllSecurableObjectPermissions(key: AclKey): Acl
    fun getAllSecurableObjectPermissions(keys: Set<AclKey>): Set<Acl>

    /**
     * Returns all Principals, which have all the specified permissions on the securable object
     *
     * @param key         The securable object
     * @param permissions Set of permission to check for
     */
    fun getAuthorizedPrincipalsOnSecurableObject(key: AclKey, permissions: EnumSet<Permission>): Set<Principal>
    fun getAuthorizedObjectsOfType(
        principal: Principal,
        objectType: SecurableObjectType,
        permissions: EnumSet<Permission>
    ): Stream<AclKey>

    fun getAuthorizedObjectsOfType(
        principal: Set<Principal>,
        objectType: SecurableObjectType,
        permissions: EnumSet<Permission>
    ): Stream<AclKey>

    @Timed
    fun getAuthorizedObjectsOfType(
        principals: Set<Principal>,
        objectType: SecurableObjectType,
        permissions: EnumSet<Permission>,
        additionalFilter: Predicate<*, *>
    ): Stream<AclKey>

    fun getAuthorizedObjectsOfTypes(
        principal: Set<Principal>,
        objectTypes: Collection<SecurableObjectType>,
        permissions: EnumSet<Permission>
    ): Stream<AclKey>

    fun getSecurableObjectOwners(key: AclKey): Set<Principal>

    @Timed
    fun getOwnersForSecurableObjects(aclKeys: Collection<AclKey>): SetMultimap<AclKey, Principal>

    @Timed
    fun deleteAllPrincipalPermissions(principal: Principal)
    @Timed
    fun listAuthorizedObjectsOfType(
        principals: Set<Principal>,
        objectType: SecurableObjectType,
        permissions: EnumSet<Permission>
    ): List<AclKey>

    fun ensureAceIsLoaded(aclKey: AclKey, principal: Principal)
}