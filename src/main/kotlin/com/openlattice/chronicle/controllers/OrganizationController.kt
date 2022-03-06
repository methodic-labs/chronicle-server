package com.openlattice.chronicle.controllers

import com.codahale.metrics.annotation.Timed
import com.openlattice.chronicle.auditing.AuditEventType
import com.openlattice.chronicle.auditing.AuditableEvent
import com.openlattice.chronicle.auditing.AuditedOperationBuilder
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.AuthorizingComponent
import com.openlattice.chronicle.authorization.READ_PERMISSION
import com.openlattice.chronicle.authorization.SecurableObjectType
import com.openlattice.chronicle.authorization.principals.Principals
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.ids.IdConstants
import com.openlattice.chronicle.organizations.ChronicleDataCollectionSettings
import com.openlattice.chronicle.organizations.ChronicleOrganizationService
import com.openlattice.chronicle.organizations.Organization
import com.openlattice.chronicle.organizations.OrganizationApi
import com.openlattice.chronicle.organizations.OrganizationApi.Companion.CONTROLLER
import com.openlattice.chronicle.organizations.OrganizationApi.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.organizations.OrganizationApi.Companion.ORGANIZATION_ID_PATH
import com.openlattice.chronicle.organizations.OrganizationSettings
import com.openlattice.chronicle.settings.AppComponent
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.util.getLastAclKeySafely
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.util.UUID
import java.util.stream.Collectors
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@RestController
@RequestMapping(CONTROLLER)
class OrganizationController @Inject constructor(
    private val storageResolver: StorageResolver,
    private val idGenerationService: HazelcastIdGenerationService,
    override val authorizationManager: AuthorizationManager,
    override val auditingManager: AuditingManager
) : AuthorizingComponent, OrganizationApi {

    companion object {
        private val logger = LoggerFactory.getLogger(OrganizationController::class.java)!!
    }

    @Inject
    private lateinit var chronicleOrganizationService: ChronicleOrganizationService

    @Timed
    @PostMapping(
        path = ["", "/"],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE],
    )
    override fun createOrganization(@RequestBody organization: Organization): UUID {
        ensureAuthenticated()
        logger.info("Creating organization with title ${organization.title}")
        organization.id = idGenerationService.getNextId()
        storageResolver.getPlatformStorage().connection.use { conn ->
            AuditedOperationBuilder<Unit>(conn, auditingManager)
                .operation { connection ->
                    chronicleOrganizationService.createOrganization(
                        connection,
                        Principals.getCurrentUser(),
                        organization
                    )
                }
                .audit {
                    listOf(
                        AuditableEvent(
                            AclKey(organization.id),
                            Principals.getCurrentSecurablePrincipal().id,
                            Principals.getCurrentUser(),
                            AuditEventType.CREATE_ORGANIZATION,
                            "",
                            organization.id,
                            UUID(0, 0),
                            mapOf()
                        )
                    )
                }
                .buildAndRun()
        }
            authorizationManager.refreshCache(AclKey(organization.id), Principals.getCurrentUser() )
        return organization.id
    }

    @Timed
    @GetMapping(
        path = [ORGANIZATION_ID_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE],
    )
    override fun getOrganization(@PathVariable(ORGANIZATION_ID) organizationId: UUID): Organization {
        ensureReadAccess(AclKey(organizationId))
        return chronicleOrganizationService.getOrganization(organizationId)
    }

    @Timed
    @GetMapping(
        path = ["", "/"],
        produces = [MediaType.APPLICATION_JSON_VALUE],
    )
    override fun getOrganizations(): Iterable<Organization> {
        ensureAuthenticated()
        val organizationIds = getAccessibleObjects(SecurableObjectType.Organization, READ_PERMISSION)
            .collect(Collectors.toSet())
            .mapNotNull { it?.firstOrNull() }
            .filter { it != IdConstants.SYSTEM_ORGANIZATION.id }
        return chronicleOrganizationService.getOrganizations(organizationIds)
    }

    override fun searchOrganizations(): Collection<Organization> {
        TODO("Not yet implemented")
    }

    override fun getOrganizationSettings(): OrganizationSettings {
        TODO("Not yet implemented")
    }

    override fun getChronicleDataCollectionSettings(organizationId: UUID): ChronicleDataCollectionSettings {
        TODO("Not yet implemented")
    }

    override fun getAppComponentSettings(organizationId: UUID, appComponent: AppComponent): Map<String, Any> {
        TODO("Not yet implemented")
    }

    override fun setOrganizationSettings(organizationId: UUID, orgSettings: OrganizationSettings) {
        TODO("Not yet implemented")
    }

    override fun setChronicleDataCollectionSettings(
        organizationId: UUID,
        dataCollectionSettings: ChronicleDataCollectionSettings
    ) {
        TODO("Not yet implemented")
    }

    override fun setAppComponentSettings(organizationId: UUID, appComponent: AppComponent, settings: Map<String, Any>) {
        TODO("Not yet implemented")
    }

}
