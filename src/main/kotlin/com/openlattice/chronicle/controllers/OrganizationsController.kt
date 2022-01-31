package com.openlattice.chronicle.controllers

import com.codahale.metrics.annotation.Timed
import com.openlattice.chronicle.auditing.AuditEventType
import com.openlattice.chronicle.auditing.AuditableEvent
import com.openlattice.chronicle.auditing.AuditedOperationBuilder
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.AuthorizingComponent
import com.openlattice.chronicle.authorization.principals.Principals
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.organizations.ChronicleDataCollectionSettings
import com.openlattice.chronicle.organizations.ChronicleOrganizationService
import com.openlattice.chronicle.organizations.Organization
import com.openlattice.chronicle.organizations.OrganizationSettings
import com.openlattice.chronicle.organizations.OrganizationsApi
import com.openlattice.chronicle.organizations.OrganizationsApi.Companion.CONTROLLER
import com.openlattice.chronicle.organizations.OrganizationsApi.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.organizations.OrganizationsApi.Companion.ORGANIZATION_ID_PATH
import com.openlattice.chronicle.settings.AppComponent
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.util.ensureVanilla
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.util.UUID
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@RestController
@RequestMapping(CONTROLLER)
class OrganizationsController @Inject constructor(
    private val storageResolver: StorageResolver,
    private val idGenerationService: HazelcastIdGenerationService,
    override val authorizationManager: AuthorizationManager,
    override val auditingManager: AuditingManager
) : AuthorizingComponent, OrganizationsApi {

    companion object {
        private val logger = LoggerFactory.getLogger(StudyController::class.java)!!
    }

    @Inject
    private lateinit var chronicleOrganizationService: ChronicleOrganizationService

    @Timed
    @PostMapping(
        path = ["", "/"],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE],
    )
    override fun createOrganization(organization: Organization): UUID {
        ensureAuthenticated()
        logger.info("Creating organization with title ${organization.title}")
        organization.id = idGenerationService.getNextId()
        val (flavor, hds) = storageResolver.getPlatformStorage()
        ensureVanilla(flavor)
        AuditedOperationBuilder<Unit>(hds.connection, auditingManager)
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
                        Principals.getCurrentUser().id,
                        AuditEventType.CREATE_ORGANIZATION,
                        "",
                        organization.id,
                        UUID(0, 0),
                        mapOf()
                    )
                )
            }
            .buildAndRun()
        return organization.id
    }

    @Timed
    @GetMapping(
        path = [ORGANIZATION_ID_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE],
    )
    override fun getOrganization(@PathVariable(ORGANIZATION_ID) organizationId: UUID): Organization {
        return chronicleOrganizationService.getOrganization(organizationId)
    }

    override fun searchOrganizations(): Collection<Organization> {
        TODO("Not yet implemented")
    }

    override fun getOrganizations(): Collection<Organization> {
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
        organizationId: UUID, dataCollectionSettings: ChronicleDataCollectionSettings
    ) {
        TODO("Not yet implemented")
    }

    override fun setAppComponentSettings(organizationId: UUID, appComponent: AppComponent, settings: Map<String, Any>) {
        TODO("Not yet implemented")
    }

}
