/*
 * Copyright (C) 2019. OpenLattice, Inc.
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
 *
 */

package com.openlattice.chronicle.authorization.initializers

import com.geekbeast.tasks.HazelcastInitializationTask
import com.geekbeast.tasks.Task
import com.openlattice.chronicle.authorization.Role
import com.openlattice.chronicle.authorization.SecurablePrincipal
import com.openlattice.chronicle.authorization.SystemRole
import com.openlattice.chronicle.authorization.SystemUser
import com.openlattice.chronicle.ids.IdConstants
import java.util.*
import java.util.concurrent.TimeUnit

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class AuthorizationInitializationTask : HazelcastInitializationTask<AuthorizationInitializationDependencies> {
    override fun initialize(dependencies: AuthorizationInitializationDependencies) {
        val spm = dependencies.securePrincipalsManager
        spm.createSecurablePrincipalIfNotExists(SystemRole.AUTHENTICATED_USER.principal, GLOBAL_USER_ROLE)
        spm.createSecurablePrincipalIfNotExists(SystemRole.ADMIN.principal, GLOBAL_ADMIN_ROLE)
        spm.createSecurablePrincipalIfNotExists(SystemRole.ANONYMOUS_USER.principal, ANONYMOUS_USER_ROLE)
        spm.createSecurablePrincipalIfNotExists(SystemUser.METHODIC.principal, METHODIC_USER_ROLE)
        val source = spm.lookup(SystemRole.AUTHENTICATED_USER.principal)
        val target = spm.lookup(SystemRole.ADMIN.principal)
        spm.addPrincipalToPrincipal(source, target)
    }

    override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
        return setOf()
    }

    override fun getInitialDelay(): Long {
        return 0
    }

    override fun getTimeUnit(): TimeUnit {
        return TimeUnit.MILLISECONDS
    }

    override fun getName(): String {
        return Task.AUTHORIZATION_BOOTSTRAP.name
    }

    override fun getDependenciesClass(): Class<out AuthorizationInitializationDependencies> {
        return AuthorizationInitializationDependencies::class.java
    }

    companion object {

        @JvmField
        val GLOBAL_USER_ROLE = Role(
                Optional.empty(),
                IdConstants.SYSTEM_ORGANIZATION.id,
                SystemRole.AUTHENTICATED_USER.principal,
                "Chronicle User Role",
                Optional.of("The default user role granted to all authenticated users of the system.")
        )

        @JvmField
        val GLOBAL_ADMIN_ROLE = Role(
            Optional.empty(),
            IdConstants.SYSTEM_ORGANIZATION.id,
            SystemRole.ADMIN.principal,
            "Global Admin Role",
            Optional.of("The global administrative role that allows management of entity data model.")
        )

        @JvmField
        val METHODIC_USER_ROLE = SecurablePrincipal(
            Optional.empty(),
            SystemUser.METHODIC.principal,
            "Anonymous User Role",
            Optional.of("The system service role for operations that do not require authentication.")
        )

        @JvmField
        val ANONYMOUS_USER_ROLE = Role(
            Optional.empty(),
            IdConstants.SYSTEM_ORGANIZATION.id,
            SystemRole.ANONYMOUS_USER.principal,
            "Anonymous User Role",
            Optional.of("The anonymous user role for operations that do not require authentication.")
        )
    }

}
