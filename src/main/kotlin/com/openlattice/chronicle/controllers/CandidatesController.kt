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
import com.openlattice.chronicle.candidates.Candidate
import com.openlattice.chronicle.candidates.CandidatesApi
import com.openlattice.chronicle.candidates.CandidatesApi.Companion.CONTROLLER
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.services.candidates.CandidatesService
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.util.ensureVanilla
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.util.UUID
import javax.inject.Inject

@RestController
@RequestMapping(CONTROLLER)
class CandidatesController @Inject constructor(
    val idGenerationService: HazelcastIdGenerationService,
    val storageResolver: StorageResolver,
    override val auditingManager: AuditingManager,
    override val authorizationManager: AuthorizationManager
) : CandidatesApi, AuthorizingComponent {

    companion object {
        private val logger = LoggerFactory.getLogger(CandidatesController::class.java)
    }

    @Inject
    private lateinit var candidatesService: CandidatesService

    @Timed
    @PostMapping(
        path = ["", "/"],
        consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun registerCandidate(@RequestBody candidate: Candidate): UUID {
        ensureAuthenticated()
        val (flavor, hds) = storageResolver.getPlatformStorage()
        ensureVanilla(flavor)
        candidate.candidateId = idGenerationService.getNextId()
        AuditedOperationBuilder<Unit>(hds.connection, auditingManager)
            .operation { connection -> candidatesService.registerCandidate(connection, candidate) }
            .audit { listOf(
                AuditableEvent(
                    AclKey(candidate.candidateId),
                    Principals.getCurrentSecurablePrincipal().id,
                    Principals.getCurrentUser().id,
                    AuditEventType.REGISTER_CANDIDATE,
                    ""
                )
            ) }
            .buildAndRun()
        return candidate.candidateId
    }
}
