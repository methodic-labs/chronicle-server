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

package com.openlattice.chronicle.organizations.initializers

import com.google.common.base.Stopwatch
import com.geekbeast.tasks.HazelcastInitializationTask
import com.geekbeast.tasks.Task
import com.openlattice.chronicle.authorization.initializers.AuthorizationInitializationTask
import com.openlattice.chronicle.authorization.initializers.AuthorizationInitializationTask.Companion.GLOBAL_ADMIN_ROLE
import com.openlattice.chronicle.ids.IdConstants
import com.openlattice.chronicle.organizations.Organization
import com.openlattice.chronicle.organizations.OrganizationConstants.Companion.SYSTEM_ORG_PRINCIPAL
import com.openlattice.chronicle.organizations.OrganizationPrincipal
import com.openlattice.chronicle.organizations.ensureVanilla
import com.openlattice.chronicle.tasks.PostConstructInitializerTaskDependencies
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.TimeUnit

private val logger = LoggerFactory.getLogger(OrganizationsInitializationTask::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class OrganizationsInitializationTask : HazelcastInitializationTask<OrganizationsInitializationDependencies> {
    override fun initialize(dependencies: OrganizationsInitializationDependencies) {
        logger.info("Running bootstrap process for organizations.")
        val sw = Stopwatch.createStarted()
        val organizationService = dependencies.organizationService
        val globalOrg = organizationService.maybeGetOrganization(IdConstants.SYSTEM_ORGANIZATION.id)

        if (globalOrg.isPresent) {
            val org = globalOrg.get()

            logger.info(
                "Expected id = {}, Actual id = {}",
                IdConstants.SYSTEM_ORGANIZATION.id,
                org.id
            )
            require(IdConstants.SYSTEM_ORGANIZATION.id == org.id) {
                "Mismatch in expected global org id and read global org id"
            }
        } else {
            val org = createGlobalOrg()
            val (flavor, hds) = dependencies.storageResolver.getPlatformStorage()
            ensureVanilla(flavor)
            hds.connection.use { connection ->
                try {
                    connection.autoCommit = false
                    //TODO: Consider auditing this
                    organizationService.createOrganization(connection, GLOBAL_ADMIN_ROLE.principal, org)
                    connection.commit()
                } catch (ex:Exception) {
                    connection.rollback()
                }
            }
        }

        logger.info("Bootstrapping for organizations took {} ms", sw.elapsed(TimeUnit.MILLISECONDS))
    }

    override fun getInitialDelay(): Long {
        return 0
    }

    override fun getTimeUnit(): TimeUnit {
        return TimeUnit.MILLISECONDS
    }

    override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
        return setOf(
            AuthorizationInitializationTask::class.java,
            PostConstructInitializerTaskDependencies.PostConstructInitializerTask::class.java,
        )
    }

    override fun getName(): String {
        return Task.ORGANIZATION_INITIALIZATION.name
    }

    override fun getDependenciesClass(): Class<out OrganizationsInitializationDependencies> {
        return OrganizationsInitializationDependencies::class.java
    }

    companion object {
        private fun createGlobalOrg(): Organization {
            val id = IdConstants.SYSTEM_ORGANIZATION.id
            val title = "Chronicle System Organization"
            return Organization(
                id,
                title,
                "Built in organization for managing system roles and permissions."
            )
        }
    }
}
