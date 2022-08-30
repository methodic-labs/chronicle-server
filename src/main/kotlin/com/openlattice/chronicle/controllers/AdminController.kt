package com.openlattice.chronicle.controllers

import com.codahale.metrics.annotation.Timed
import com.geekbeast.rhizome.jobs.HazelcastJobService
import com.hazelcast.core.HazelcastInstance
import com.openlattice.chronicle.admin.AdminApi
import com.openlattice.chronicle.admin.AdminApi.Companion.CONTROLLER
import com.openlattice.chronicle.admin.AdminApi.Companion.ID
import com.openlattice.chronicle.admin.AdminApi.Companion.ID_PATH
import com.openlattice.chronicle.admin.AdminApi.Companion.NAME
import com.openlattice.chronicle.admin.AdminApi.Companion.NAME_PATH
import com.openlattice.chronicle.admin.AdminApi.Companion.PRINCIPALS
import com.openlattice.chronicle.admin.AdminApi.Companion.REDSHIFT
import com.openlattice.chronicle.admin.AdminApi.Companion.RELOAD_CACHE
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.AuthorizingComponent
import com.openlattice.chronicle.authorization.Principal
import com.openlattice.chronicle.authorization.principals.Principals
import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.openlattice.chronicle.services.upload.AppDataUploadService
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import javax.inject.Inject

@SuppressFBWarnings(
    value = ["RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE", "BC_BAD_CAST_TO_ABSTRACT_COLLECTION"],
    justification = "Allowing redundant kotlin null check on lateinit variables, " +
            "Allowing kotlin collection mapping cast to List"
)
@RestController
@RequestMapping(CONTROLLER)
class AdminController(
    override val authorizationManager: AuthorizationManager,
    override val auditingManager: AuditingManager
) : AdminApi, AuthorizingComponent {
    companion object {
        private val logger = LoggerFactory.getLogger(AdminController::class.java)!!
    }

    @Inject
    private lateinit var hazelcast: HazelcastInstance

    @Inject
    private lateinit var appDataUploadService: AppDataUploadService
    @Timed
    @GetMapping(value = [REDSHIFT])
    override fun moveToRedshift() {
        ensureAdminAccess()
        appDataUploadService.moveToRedshift()
    }

    @Timed
    @GetMapping(value = [RELOAD_CACHE])
    @SuppressFBWarnings("NP_ALWAYS_NULL", justification = "Issue with spotbugs handling of Kotlin")
    override fun reloadCache() {
        ensureAdminAccess()
        HazelcastMap.values().forEach {
            logger.info("Reloading map $it")
            try {
                it.getMap(hazelcast).loadAll(true)
            } catch (e: IllegalArgumentException) {
                logger.error("Unable to reload map $it", e)
            }
        }
    }

    @Timed
    @GetMapping(value = [RELOAD_CACHE + NAME_PATH])
    override fun reloadCache(@PathVariable(NAME) name: String) {
        ensureAdminAccess()
        HazelcastMap.valueOf(name).getMap(hazelcast).loadAll(true)
    }

    @Timed
    @GetMapping(value = [PRINCIPALS + ID_PATH])
    override fun getUserPrincipals(@PathVariable(ID) principalId: String): Set<Principal> {
        ensureAdminAccess()
        return Principals.getUserPrincipals(principalId)
    }

    @Timed
    @GetMapping(value = [PRINCIPALS])
    override fun getCurrentUserPrincipals(): Set<Principal> {
        ensureAuthenticated()
        return Principals.getCurrentPrincipals()
    }

}
