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
package com.openlattice.chronicle.controllers

import com.auth0.exception.Auth0Exception
import com.auth0.json.mgmt.users.User
import com.codahale.metrics.annotation.Timed
import com.google.common.base.Preconditions
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.*
import com.openlattice.chronicle.authorization.principals.Principals
import com.openlattice.chronicle.authorization.principals.SecurePrincipalsManager
import com.openlattice.chronicle.base.OK
import com.openlattice.chronicle.base.OK.Companion.ok
import com.openlattice.chronicle.directory.UserDirectoryService
import com.openlattice.chronicle.organizations.ChronicleOrganizationService
import com.openlattice.chronicle.users.*
import com.openlattice.chronicle.users.PrincipalApi.*
import org.springframework.http.MediaType
import org.springframework.security.authentication.BadCredentialsException
import org.springframework.web.bind.annotation.*
import java.util.*
import java.util.stream.Collectors
import kotlin.streams.asSequence

@RestController
@RequestMapping(PrincipalApi.CONTROLLER)
class PrincipalDirectoryController(
    override val authorizationManager: AuthorizationManager,
    val userDirectoryService: UserDirectoryService,
    val userListingService: UserListingService,
    val spm: SecurePrincipalsManager,
    val syncService: Auth0SyncService,
    val organizationService: ChronicleOrganizationService,
    override val auditingManager: AuditingManager
) : PrincipalApi, AuthorizingComponent {


    @Timed
    @RequestMapping(method = [RequestMethod.POST], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getSecurablePrincipal(@RequestBody principal: Principal): SecurablePrincipal {
        val aclKey: AclKey = spm.lookup(principal)
        
        //TODO: Should we check read access if a user?
        if (principal.type != PrincipalType.USER) {
            ensureReadAccess(aclKey)
        } 
        return spm.getSecurablePrincipal(aclKey)
    }

    @RequestMapping(
        path = [USERS],
        method = [RequestMethod.GET],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    @Timed
    override fun getAllUsers(): Map<String, User> = userDirectoryService.getAllUsers()

    @RequestMapping(
        path = [ROLES + CURRENT],
        method = [RequestMethod.GET],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    @Timed
    override fun getCurrentRoles(): MutableSet<SecurablePrincipal> {
        return Principals.getCurrentPrincipals()
            .stream()
            .filter { principal -> principal.type == PrincipalType.ROLE }
            .map(spm::lookup)
            .filter { obj: Any? -> Objects.nonNull(obj) }
            .map { aclKey -> spm.getSecurablePrincipal(aclKey) }
            .collect(Collectors.toSet())
    }

    @RequestMapping(
        path = [ROLES],
        method = [RequestMethod.GET],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    @Timed
    override fun getAvailableRoles(): MutableMap<AclKey, Role> {
        return authorizationManager
            .getAuthorizedObjectsOfType(
                Principals.getCurrentPrincipals(),
                SecurableObjectType.Role,
                EnumSet.of(Permission.READ)
            )
            .asSequence()
            .associateWithTo(mutableMapOf()) { spm.getSecurablePrincipal(it) as Role }
    }

    @Timed
    @RequestMapping(
        path = [USERS + USER_ID_PATH],
        method = [RequestMethod.GET],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getUser(@PathVariable(USER_ID) userId: String): User {
        return userDirectoryService.getUser(userId)
    }

    @Timed
    @RequestMapping(
        path = [USERS],
        method = [RequestMethod.POST],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getUsers(@RequestBody userIds: Set<String>): Map<String, User> {
        return userDirectoryService.getUsers(userIds)
    }

    @Timed
    @RequestMapping(path = [SYNC], method = [RequestMethod.GET])
    override fun syncCallingUser(): OK {
        /*
         * Important note: getCurrentUser() reads the principal id directly from auth token.
         *
         * This is safe since token has been validated and has an auth0 assigned unique id.
         *
         * It is very important that this is the *first* call for a new user.
         */
        val principal: Principal = Preconditions.checkNotNull(Principals.getCurrentUser())
        try {
            val user = userListingService.getUser( principal.id )
            syncService.syncUser(user)
        } catch (e: IllegalArgumentException) {
            throw BadCredentialsException("Unable to retrieve user profile information from auth0", e)
        } catch (e: Auth0Exception) {
            throw BadCredentialsException("Unable to retrieve user profile information from auth0", e)
        }

        return ok
    }

    @Timed
    @PostMapping(
        path = [USERS + SEARCH],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun searchUsers(@RequestBody fields: Auth0UserSearchFields): Map<String, User> {
        return userDirectoryService.searchAllUsers(fields)
    }

    @Timed
    @PostMapping(path = [UPDATE], consumes = [MediaType.APPLICATION_JSON_VALUE])
    override fun addPrincipalToPrincipal(@RequestBody directedAclKeys: DirectedAclKeys): OK {
        ensureWriteAccess(directedAclKeys.target)
        ensureOwnerAccess(directedAclKeys.source)
        spm.addPrincipalToPrincipal(directedAclKeys.source, directedAclKeys.target)
        return ok
    }

    @Timed
    @DeleteMapping(path = [UPDATE], consumes = [MediaType.APPLICATION_JSON_VALUE])
    override fun removePrincipalFromPrincipal(@RequestBody directedAclKeys: DirectedAclKeys): OK {
        ensureWriteAccess(directedAclKeys.target)
        ensureOwnerAccess(directedAclKeys.source)
        spm.removePrincipalFromPrincipal(directedAclKeys.source, directedAclKeys.target)
        return ok
    }

    @DeleteMapping(path = [USERS + USER_ID_PATH])
    override fun deleteUserAccount(@PathVariable(USER_ID) userId: String): OK {
        ensureAdminAccess()

        //First remove from all organizations
        organizationService.removeMemberFromAllOrganizations(Principal(PrincipalType.USER, userId))
        val securablePrincipal: SecurablePrincipal = spm.getSecurablePrincipal(userId)
        spm.deletePrincipal(securablePrincipal.aclKey)
        authorizationManager.deleteAllPrincipalPermissions(Principal(PrincipalType.USER, userId))

        //Delete from auth0
        userDirectoryService.deleteUser(userId)
        return ok
    }
}