package com.openlattice.chronicle.controllers

import com.codahale.metrics.annotation.Timed
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.AuthorizingComponent
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.people.Person
import com.openlattice.chronicle.people.PeopleApi
import com.openlattice.chronicle.people.PeopleApi.Companion.CONTROLLER
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
class PeopleController @Inject constructor(
    val idGenerationService: HazelcastIdGenerationService,
    val storageResolver: StorageResolver,
    override val auditingManager: AuditingManager,
    override val authorizationManager: AuthorizationManager
) : PeopleApi, AuthorizingComponent {

    companion object {
        private val logger = LoggerFactory.getLogger(PeopleController::class.java)
    }

    @Timed
    @PostMapping(
        path = ["", "/"],
        consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun createPerson(person: Person): UUID {
        throw NotImplementedException("coming soon")
    }
}
