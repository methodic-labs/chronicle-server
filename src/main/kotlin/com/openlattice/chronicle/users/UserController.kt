package com.openlattice.chronicle.users

import com.auth0.client.mgmt.ManagementAPI
import com.codahale.metrics.annotation.Timed
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.AuthorizingComponent
import com.openlattice.chronicle.authorization.principals.Principals
import com.openlattice.chronicle.users.UserApi.Companion.CONTROLLER
import com.openlattice.chronicle.users.UserApi.Companion.SYNC_PATH
import com.openlattice.users.getUser
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import javax.inject.Inject

@RestController
@RequestMapping(CONTROLLER)
class UserController @Inject constructor(
    override val auditingManager: AuditingManager,
    override val authorizationManager: AuthorizationManager
) : UserApi, AuthorizingComponent {

    @Inject
    private lateinit var auth0ManagementApi: ManagementAPI

    @Inject
    private lateinit var auth0SyncService: Auth0SyncService

    @Timed
    @GetMapping(path = [SYNC_PATH])
    override fun sync() {
        try {
            val principal = Principals.getCurrentUser()
            val user = getUser(auth0ManagementApi, principal.id)
            auth0SyncService.syncUser(user)
        }
        catch (e: Exception) {
            throw RuntimeException("unable to sync user", e)
        }
    }
}
