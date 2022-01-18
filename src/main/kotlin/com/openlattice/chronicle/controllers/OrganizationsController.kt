package com.openlattice.chronicle.controllers

import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.AuthorizingComponent
import com.openlattice.chronicle.organizations.*
import com.openlattice.chronicle.organizations.OrganizationsApi.Companion.CONTROLLER
import com.openlattice.chronicle.settings.AppComponent
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.util.*
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@RestController
@RequestMapping(CONTROLLER)
class OrganizationsController @Inject constructor(
        override val authorizationManager: AuthorizationManager
) : AuthorizingComponent, OrganizationsApi {

    @Inject
    private lateinit var chronicleOrganizationService:  ChronicleOrganizationService

    override fun createOrganization(organizationPrincipal: OrganizationPrincipal): UUID {
        ensureAuthenticated()
        return chronicleOrganizationService.createOrganization(organizationPrincipal)
    }

    override fun searchOrganizations(): Collection<OrganizationPrincipal> {
        TODO("Not yet implemented")
    }

    override fun getOrganizations(): Collection<OrganizationPrincipal> {
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