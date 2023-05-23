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

import com.codahale.metrics.annotation.Timed
import com.geekbeast.controllers.exceptions.BadRequestException
import com.geekbeast.controllers.exceptions.ForbiddenException
import com.geekbeast.streams.StreamUtil
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.google.common.collect.Lists
import com.openlattice.chronicle.auditing.AuditEventType
import com.openlattice.chronicle.auditing.AuditableEvent
import com.openlattice.chronicle.auditing.AuditingComponent
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.*
import com.openlattice.chronicle.authorization.principals.Principals
import com.openlattice.chronicle.authorization.principals.SecurePrincipalsManager
import com.openlattice.chronicle.base.OK
import com.openlattice.chronicle.base.OK.Companion.ok
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RestController
import java.util.*
import java.util.function.Function
import java.util.stream.Collectors
import javax.inject.Inject

@RestController
@RequestMapping(PermissionsApi.CONTROLLER)
class PermissionsController @Inject constructor(
    val securePrincipalsManager: SecurePrincipalsManager,
    override val auditingManager: AuditingManager,
    override val authorizationManager: AuthorizationManager
) : PermissionsApi, AuthorizingComponent {


    @Timed
    @RequestMapping(path = ["", "/"], method = [RequestMethod.PATCH], consumes = [MediaType.APPLICATION_JSON_VALUE])
    override fun updateAcl(@RequestBody req: AclData): OK {
        return updateAcls(ImmutableList.of(req))
    }

    @Timed
    @RequestMapping(
        path = [PermissionsApi.UPDATE],
        method = [RequestMethod.PATCH],
        consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun updateAcls(@RequestBody req: List<AclData>): OK {
        val requestsByActionType = req.groupBy({ it.action }) { it.acl }

        /*
         * Ensure that the user has alter permissions on Acl permissions being modified
         */
        val unauthorizedAclKeys: Set<AclKey> = authorizationManager.accessChecksForPrincipals(
            requestsByActionType.entries
                .filter { it.key != Action.REQUEST }
                .flatMap { it.value }
                .map { acl -> AccessCheck(acl.aclKey, EnumSet.of(Permission.OWNER)) }
                .toSet(),
            Principals.getCurrentPrincipals())
            .filter { authorization -> !(authorization.permissions[Permission.OWNER] ?: false) }
            .map(Authorization::aclKey)
            .toSet()
        if (unauthorizedAclKeys.isNotEmpty()) {
            throw ForbiddenException(
                "Only owner of securable objects " + unauthorizedAclKeys +
                        " can access other users' access rights."
            )
        }
        requestsByActionType.forEach { (action: Action, acls: List<Acl>) ->
            val eventType = when (action) {
                Action.ADD -> {
                    authorizationManager!!.addPermissions(acls)
                    AuditEventType.ADD_PERMISSION
                }
                Action.REMOVE -> {
                    authorizationManager!!.removePermissions(acls)
                    AuditEventType.REMOVE_PERMISSION
                }
                Action.SET -> {
                    authorizationManager!!.setPermissions(acls)
                    AuditEventType.SET_PERMISSION
                }
                else -> {
                    logger.error("Invalid action {} specified for request.", action)
                    throw BadRequestException("Invalid action specified: $action")
                }
            }
            recordEvents(createAuditableEvents(acls, eventType))
        }
        return ok
    }

    @Timed
    @RequestMapping(
        path = ["", "/"],
        method = [RequestMethod.POST],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getAcl(@RequestBody aclKeys: AclKey): Acl {
        if (!isAuthorized(Permission.OWNER).test(aclKeys)) {
            throw ForbiddenException(
                "Only owner of securable object " + aclKeys +
                        " can access other users' access rights."
            )
        }
        return authorizationManager.getAllSecurableObjectPermissions(aclKeys)
    }

    @Timed
    @RequestMapping(
        path = [PermissionsApi.BULK],
        method = [RequestMethod.POST],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getAcls(@RequestBody keys: Set<AclKey>): Set<Acl> {
        ensureOwnerAccess(keys)
        return authorizationManager.getAllSecurableObjectPermissions(keys)
    }

    @Timed
    @RequestMapping(
        path = [PermissionsApi.EXPLAIN],
        method = [RequestMethod.POST],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    @SuppressFBWarnings(value = ["NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE"], justification = "https://github.com/spotbugs/spotbugs/issues/927")
    override fun getAclExplanation(@RequestBody aclKey: AclKey): Collection<AclExplanation> {
        ensureOwnerAccess(aclKey)

        //maps aces to principal type
        val aces = authorizationManager.getAllSecurableObjectPermissions(aclKey).aces //gets aces from returned acl
        val acesByType = StreamUtil.stream(aces)
            .collect(
                Collectors.groupingBy(
                    Function { (principal): Ace -> principal.type })
            )

        //maps non-user principals to a List<List<Principal>> containing one list of themselves
        val aceStream = acesByType.entries
            .filter { it.key != PrincipalType.USER }
            .flatMap { it.value }

        val principalToPrincipalPaths = aceStream.associateByTo(mutableMapOf(), { it.principal }) {
            mutableListOf<MutableList<Principal>>()
        }

        principalToPrincipalPaths.forEach { (p, pl) ->
            val path = mutableListOf(p)
            pl.add(path)
        }

        //maps all principals to principals path that grant permission on the acl key
        var currentLayer: Set<Principal> = HashSet(principalToPrincipalPaths.keys)
        while (currentLayer.isNotEmpty()) { //while we have nodes to get paths for
            val parentLayer: MutableSet<Principal> = HashSet()
            for (p in currentLayer) {
                val childPaths: List<List<Principal>> = principalToPrincipalPaths[p]!!
                val currentParents = securePrincipalsManager
                    .getParentPrincipalsOfPrincipal(securePrincipalsManager.lookup(p)).stream()
                    .map(SecurablePrincipal::principal)
                    .collect(Collectors.toSet())

                //removes self-loops
                currentParents.remove(p)
                for (parent in currentParents) {
                    val paths = principalToPrincipalPaths.getOrDefault(parent, ArrayList())

                    //if map doesn't contain entry for parent, add it to map with current empty paths object
                    if (paths.isEmpty()) {
                        principalToPrincipalPaths[parent] = paths
                    }

                    //build paths
                    for (path in childPaths) {
                        val newPath = ArrayList(path)
                        newPath.add(parent)
                        if (!paths.contains(newPath)) {
                            paths.add(newPath)
                        }
                    }
                }
                parentLayer.addAll(currentParents)
            }
            currentLayer = parentLayer
        }

        //collect map entries as aclExplanations
        return principalToPrincipalPaths
            .entries
            .stream()
            .map { (key, value): Map.Entry<Principal, List<List<Principal>>> ->
                AclExplanation(
                    key, value
                )
            }
            .collect(Collectors.toSet())
    }

    private fun createAuditableEvents(acls: List<Acl>, eventType: AuditEventType): List<AuditableEvent> {
        return acls.map { (aclKey, aces): Acl ->
            AuditableEvent(
                aclKey,
                eventType = eventType,
                description = "Permissions updated through PermissionApi.updateAcl",
                data = ImmutableMap.of("aces", aces)
            )
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(PermissionsController::class.java)
    }
}