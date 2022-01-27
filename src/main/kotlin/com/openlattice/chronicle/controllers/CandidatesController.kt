package com.openlattice.chronicle.controllers

import com.codahale.metrics.annotation.Timed
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.AuthorizingComponent
import com.openlattice.chronicle.candidates.Candidate
import com.openlattice.chronicle.candidates.CandidatesApi
import com.openlattice.chronicle.candidates.CandidatesApi.Companion.CONTROLLER
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.storage.StorageResolver
import org.apache.commons.lang3.NotImplementedException
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.PostMapping
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

    @Timed
    @PostMapping(
        path = ["", "/"],
        consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun createCandidate(candidate: Candidate): UUID {
        throw NotImplementedException("coming soon")
    }
}
