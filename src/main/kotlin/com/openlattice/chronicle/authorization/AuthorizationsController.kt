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
import com.openlattice.chronicle.authorization.principals.Principals
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.util.EnumSet
import java.util.stream.Collectors
import javax.inject.Inject

@RestController
@RequestMapping(AuthorizationsApi.CONTROLLER)
class AuthorizationsController : AuthorizationsApi, AuthorizingComponent {
    @Inject
    override lateinit var authorizationManager: AuthorizationManager

    @Timed
    @RequestMapping(
            method = [RequestMethod.POST], consumes = [MediaType.APPLICATION_JSON_VALUE],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun checkAuthorizations(@RequestBody queries: Set<AccessCheck>): Iterable<Authorization> {
        return Iterable {
            authorizationManager.accessChecksForPrincipals(
                    queries, Principals.currentPrincipals
            ).iterator()
        }
    }

    @Timed
    @RequestMapping(method = [RequestMethod.GET], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getAccessibleObjects(
            @RequestParam(value = AuthorizationsApi.OBJECT_TYPE) objectType: SecurableObjectType,
            @RequestParam(value = AuthorizationsApi.PERMISSION) permission: Permission,
            @RequestParam(value = AuthorizationsApi.PAGING_TOKEN, required = false) pagingToken: String
    ): AuthorizedObjectsSearchResult {
        val authorizedAclKeys = authorizationManager.getAuthorizedObjectsOfType(
            Principals.currentPrincipals,
            objectType,
            EnumSet.of(permission)
        ).collect(Collectors.toSet())
        return AuthorizedObjectsSearchResult("", authorizedAclKeys)
    }
}