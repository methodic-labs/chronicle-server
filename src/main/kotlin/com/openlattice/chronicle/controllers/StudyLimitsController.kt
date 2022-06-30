package com.openlattice.chronicle.controllers

import com.openlattice.chronicle.auditing.*
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.AuthorizingComponent
import com.openlattice.chronicle.services.studies.StudyLimitsManager
import com.openlattice.chronicle.services.studies.StudyService
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.study.StudyLimits
import com.openlattice.chronicle.study.StudyLimitsApi
import com.openlattice.chronicle.study.StudyLimitsApi.Companion.STUDY
import com.openlattice.chronicle.study.StudyLimitsApi.Companion.STUDY_ID
import com.openlattice.chronicle.study.StudyLimitsApi.Companion.STUDY_ID_PATH
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.util.*
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@RestController
@RequestMapping(StudyLimitsApi.CONTROLLER)
class StudyLimitsController @Inject constructor(
    private val studyLimitsMgr: StudyLimitsManager,
    private val studyService: StudyService,
    private val storageResolver: StorageResolver,
    override val auditingManager: AuditingManager,
    override val authorizationManager: AuthorizationManager
) : StudyLimitsApi, AuthorizingComponent {
    @PutMapping(
        value = [STUDY + STUDY_ID_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun setStudyLimits(@PathVariable(STUDY_ID) studyId: UUID, @RequestBody studyLimits: StudyLimits) {
        ensureAdminAccess()
        storageResolver.getPlatformStorage().connection.use { connection ->
            AuditedTransactionBuilder<Unit>(connection, auditingManager)
                .transaction {
                    studyLimitsMgr.setStudyLimits(studyId, studyLimits)
                }.audit {
                    listOf(
                        AuditableEvent(
                            AclKey(studyId),
                            eventType = AuditEventType.SET_STUDY_LIMITS,
                            description = "Set study limits."
                        )
                    )
                }.buildAndRun()
        }
    }

    @GetMapping(
        value = [STUDY + STUDY_ID_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getStudyLimits(@PathVariable(STUDY_ID) studyId: UUID): StudyLimits {
        ensureReadAccess(AclKey(studyId))
        check( studyService.isValidStudy(studyId) ) { "$studyId is not valid." }
        return studyLimitsMgr.getStudyLimits(studyId)
    }
}