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
import com.openlattice.chronicle.candidates.CandidateApi
import com.openlattice.chronicle.candidates.CandidateApi.Companion.BULK_PATH
import com.openlattice.chronicle.candidates.CandidateApi.Companion.CANDIDATE_ID_PARAM
import com.openlattice.chronicle.candidates.CandidateApi.Companion.CANDIDATE_ID_PATH
import com.openlattice.chronicle.candidates.CandidateApi.Companion.CONTROLLER
import com.openlattice.chronicle.services.candidates.CandidateService
import com.openlattice.chronicle.storage.StorageResolver
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.util.UUID
import javax.inject.Inject

@RestController
@RequestMapping(CONTROLLER)
class CandidateController @Inject constructor(
    val storageResolver: StorageResolver,
    override val auditingManager: AuditingManager,
    override val authorizationManager: AuthorizationManager
) : CandidateApi, AuthorizingComponent {

    companion object {
        private val logger = LoggerFactory.getLogger(CandidateController::class.java)
    }

    @Inject
    private lateinit var candidateService: CandidateService

    @Timed
    @GetMapping(
        path = [CANDIDATE_ID_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getCandidate(@PathVariable(CANDIDATE_ID_PARAM) candidateId: UUID): Candidate {
        ensureAuthenticated()
        ensureReadAccess(AclKey(candidateId))
        return try {
            candidateService.getCandidate(candidateId)
        }
        catch (e: NoSuchElementException) {
            throw CandidateNotFoundException(candidateId)
        }
    }

    @Timed
    @PostMapping(
        path = [BULK_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getCandidates(@RequestBody candidateIds: Set<UUID>): Iterable<Candidate> {
        ensureAuthenticated()
        ensureReadAccess(candidateIds.mapTo(mutableSetOf()) { AclKey(it) })
        return candidateService.getCandidates(candidateIds)
    }

    @Timed
    @PostMapping(
        path = ["", "/"],
        consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun registerCandidate(@RequestBody candidate: Candidate): UUID {
        ensureAuthenticated()
        val hds = storageResolver.getPlatformStorage()
        return AuditedOperationBuilder<UUID>(hds.connection, auditingManager)
            .operation { connection -> candidateService.registerCandidate(connection, candidate) }
            .audit { candidateId ->
                listOf(
                    AuditableEvent(
                        AclKey(candidateId),
                        Principals.getCurrentSecurablePrincipal().id,
                        Principals.getCurrentUser().id,
                        AuditEventType.REGISTER_CANDIDATE,
                        ""
                    )
                )
            }
            .buildAndRun()
    }
}
