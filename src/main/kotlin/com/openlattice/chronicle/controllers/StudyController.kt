package com.openlattice.chronicle.controllers

import com.codahale.metrics.annotation.Timed
import com.geekbeast.configuration.postgres.PostgresFlavor
import com.openlattice.chronicle.auditing.*
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.AuthorizingComponent
import com.openlattice.chronicle.authorization.Permission
import com.openlattice.chronicle.authorization.principals.Principals
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.ids.IdConstants
import com.openlattice.chronicle.study.StudyApi.Companion.CONTROLLER
import com.openlattice.chronicle.services.studies.StudyService
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.study.Study
import com.openlattice.chronicle.study.StudyApi
import com.openlattice.chronicle.study.StudyApi.Companion.STUDY_ID
import com.openlattice.chronicle.study.StudyApi.Companion.STUDY_ID_PATH
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.util.*
import javax.inject.Inject
import kotlin.NoSuchElementException


/**
 * @author Solomon Tang <solomon@openlattice.com>
 */

@RestController
@RequestMapping(CONTROLLER)
class StudyController @Inject constructor(
    val storageResolver: StorageResolver,
    val idGenerationService: HazelcastIdGenerationService,
    override val authorizationManager: AuthorizationManager,
    override val auditingManager: AuditingManager
) : StudyApi, AuthorizingComponent {
    @Inject
    private lateinit var studyService: StudyService

    companion object {
        private val logger = LoggerFactory.getLogger(StudyController::class.java)!!
    }

    @Timed
    @PostMapping(
        path = ["", "/"],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE],
    )
    override fun createStudy(@RequestBody study: Study): UUID {
        ensureAuthenticated()
        logger.info("Creating study associated with organizations ${study.organizationIds}")
        val (flavor, hds) = storageResolver.getPlatformStorage()
        check(flavor == PostgresFlavor.VANILLA) { "Only vanilla postgres supported for studies." }
        study.id = idGenerationService.getNextId()
        AuditedOperationBuilder<Unit>(hds.connection, auditingManager)
            .operation { connection -> studyService.createStudy(connection, study) }
            .audit {
                listOf(
                    AuditableEvent(
                        AclKey(study.id),
                        Principals.getCurrentSecurablePrincipal().id,
                        Principals.getCurrentUser().id,
                        AuditEventType.CREATE_STUDY,
                        "",
                        study.id,
                        UUID(0, 0),
                        mapOf()
                    )
                ) + study.organizationIds.map { organizationId ->
                    AuditableEvent(
                        AclKey(study.id),
                        Principals.getCurrentSecurablePrincipal().id,
                        Principals.getCurrentUser().id,
                        AuditEventType.CREATE_STUDY,
                        "",
                        study.id,
                        organizationId,
                        mapOf()
                    )
                }
            }
            .buildAndRun()

        return study.id
    }

    @Timed
    @GetMapping(
        path = [STUDY_ID_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE],
    )
    override fun getStudy(@PathVariable(STUDY_ID) studyId: UUID): Study {
        accessCheck(AclKey(studyId), EnumSet.of(Permission.READ))
        logger.info("Retrieving study with id $studyId")

        return try {
            val study = studyService.getStudy(listOf(studyId)).first()
            recordEvent(
                AuditableEvent(
                    AclKey(studyId),
                    Principals.getCurrentSecurablePrincipal().id,
                    Principals.getCurrentUser().id,
                    AuditEventType.GET_STUDY,
                    "",
                    studyId,
                    IdConstants.UNINITIALIZED.id,
                    mapOf()
                )
            )
            study
        } catch (ex: NoSuchElementException) {
            throw StudyNotFoundException(studyId, "No study with id $studyId found.")
        }

    }

    @Timed
    @PutMapping(
        path = [STUDY_ID_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun updateStudy(
        @PathVariable(STUDY_ID) studyId: UUID,
        @RequestBody study: Study
    ) {
        val studyAclKey = AclKey(studyId);
        ensureOwnerAccess(studyAclKey)
        val currentUserId = Principals.getCurrentUser().id;
        logger.info("Updating study with id $studyId on behalf of $currentUserId")

        val (flavor, hds) = storageResolver.getPlatformStorage()
        check(flavor == PostgresFlavor.VANILLA) { "Only vanilla postgres supported for studies." }
        AuditedOperationBuilder<Unit>(hds.connection, auditingManager)
            .operation { connection -> studyService.updateStudy(connection, studyId, study) }
            .audit {
                listOf(
                    AuditableEvent(
                        studyAclKey,
                        Principals.getCurrentSecurablePrincipal().id,
                        currentUserId,
                        AuditEventType.UPDATE_STUDY,
                        "",
                        studyId,
                        UUID(0, 0),
                        mapOf()
                    )
                )
            }
    }

}