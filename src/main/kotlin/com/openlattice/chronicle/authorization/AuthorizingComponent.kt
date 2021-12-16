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

import com.openlattice.chronicle.authorization.principals.Principals
import com.openlattice.controllers.exceptions.ForbiddenException
import org.slf4j.LoggerFactory
import java.util.*
import java.util.function.Function
import java.util.function.Predicate
import java.util.stream.Collectors
import java.util.stream.Stream

val TRANSPORT_PERMISSION: EnumSet<Permission> = EnumSet.of<Permission>(Permission.MATERIALIZE)
val READ_PERMISSION: EnumSet<Permission> = EnumSet.of<Permission>(Permission.READ)
val WRITE_PERMISSION: EnumSet<Permission> = EnumSet.of<Permission>(Permission.WRITE)
val OWNER_PERMISSION: EnumSet<Permission> = EnumSet.of<Permission>(Permission.OWNER)
val INTEGRATE_PERMISSION: EnumSet<Permission> = EnumSet.of<Permission>(Permission.INTEGRATE)
private val internalIds: Set<UUID> = setOf() //Reserved for future use.

interface AuthorizingComponent {
    companion object {
        val logger = LoggerFactory.getLogger(AuthorizingComponent::class.java)
    }

    val authorizationManager: AuthorizationManager

    fun <T : AbstractSecurableObject> isAuthorizedObject(
            requiredPermission: Permission,
            vararg requiredPermissions: Permission
    ): Predicate<T> {
        return Predicate { abs: T ->
            isAuthorized(requiredPermission, *requiredPermissions)
                    .test(AclKey(abs.id))
        }
    }

    fun isAuthorized(
            requiredPermission: Permission,
            vararg requiredPermissions: Permission
    ): Predicate<AclKey> {
        return isAuthorized(EnumSet.of<Permission>(requiredPermission, *requiredPermissions))
    }

    fun isAuthorized(requiredPermissions: EnumSet<Permission>): Predicate<AclKey> {
        return Predicate { aclKey: AclKey ->
            authorizationManager.checkIfHasPermissions(
                    aclKey,
                    Principals.getCurrentPrincipals(),
                    requiredPermissions
            )
        }
    }

    fun owns(aclKey: AclKey): Boolean {
        return isAuthorized(Permission.OWNER).test(
                AclKey(
                        aclKey!!
                )
        )
    }

    fun ensureReadAccess(aclKey: AclKey) {
        accessCheck(aclKey, READ_PERMISSION)
    }

    fun ensureIntegrateAccess(aclKey: AclKey) {
        accessCheck(aclKey, INTEGRATE_PERMISSION)
    }

    fun ensureReadAccess(keys: Set<AclKey>) {
        accessCheck(
                keys.stream().collect(
                        Collectors.toMap(Function.identity(), Function { aclKey: AclKey -> READ_PERMISSION })
                )
        )
    }

    fun ensureWriteAccess(aclKey: AclKey) {
        accessCheck(aclKey, WRITE_PERMISSION)
    }

    fun ensureOwnerAccess(aclKey: AclKey) {
        accessCheck(aclKey, EnumSet.of<Permission>(Permission.OWNER))
    }

    fun ensureOwnerAccess(keys: Set<AclKey>) {
        val owner: EnumSet<Permission> = EnumSet.of<Permission>(Permission.OWNER)
        accessCheck(
                keys.stream().collect(
                        Collectors.toMap<AclKey, AclKey, EnumSet<Permission>>(
                                Function.identity(),
                                Function<AclKey, EnumSet<Permission>> { aclKey: AclKey -> owner })
                )
        )
    }

    fun ensureLinkAccess(aclKey: AclKey) {
        accessCheck(aclKey, EnumSet.of<Permission>(Permission.LINK))
    }

    val isAdmin: Boolean
        get() = Principals.getCurrentPrincipals().contains(
                Principals.getAdminRole())

    fun ensureAdminAccess() {
        if (!isAdmin) {
            throw ForbiddenException("Only admins are allowed to perform this action.")
        }
    }

    fun authorize(
            requiredPermissionsByAclKey: Map<AclKey, EnumSet<Permission>>,
            principals: Set<Principal> = Principals.getCurrentPrincipals()
    ): Map<AclKey, EnumMap<Permission, Boolean>> {
        return authorizationManager.authorize(requiredPermissionsByAclKey, principals)
    }

    fun accessCheck(requiredPermissionsByAclKey: Map<AclKey, EnumSet<Permission>>) {
        val authorized = authorizationManager
                .authorize(requiredPermissionsByAclKey, Principals.getCurrentPrincipals())
                .values.stream().allMatch(
                        Predicate { m: EnumMap<Permission, Boolean> ->
                            m.values.stream().allMatch(
                                    Predicate { v: Boolean -> v!! })
                        })
        if (!authorized) {
            logger.warn("Authorization failed for required permissions: {}", requiredPermissionsByAclKey)
            throw ForbiddenException("Insufficient permissions to perform operation.")
        }
    }

    fun accessCheck(aclKey: AclKey, requiredPermissions: EnumSet<Permission>) {
        if (!authorizationManager.checkIfHasPermissions(
                        aclKey,
                        Principals.getCurrentPrincipals(),
                        requiredPermissions
                )) {
            throw ForbiddenException("Object $aclKey is not accessible.")
        }
    }

    fun getAccessibleObjects(
            securableObjectType: SecurableObjectType,
            requiredPermissions: EnumSet<Permission>
    ): Stream<AclKey> {
        return authorizationManager.getAuthorizedObjectsOfType(
                Principals.getCurrentPrincipals(),
                securableObjectType,
                requiredPermissions
        )
    }

    fun getAccessibleObjects(
            securableObjectType: SecurableObjectType,
            requiredPermissions: EnumSet<Permission>,
            additionalFilters: com.hazelcast.query.Predicate<*, *>
    ): Stream<AclKey> {
        return authorizationManager.getAuthorizedObjectsOfType(
                Principals.getCurrentPrincipals(),
                securableObjectType,
                requiredPermissions,
                additionalFilters
        )
    }

    fun ensureObjectCanBeDeleted(objectId: UUID) {
        if (internalIds.contains(objectId)) {
            throw ForbiddenException(
                    "Object " + objectId.toString() + " cannot be deleted because this id is reserved."
            )
        }
    }
}