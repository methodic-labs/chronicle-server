package com.openlattice.chronicle.controllers

import com.openlattice.chronicle.auditing.*
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.AuthorizingComponent
import com.openlattice.chronicle.base.OK
import com.openlattice.chronicle.study.ComplianceViolation
import com.openlattice.chronicle.study.StudyComplianceManager
import com.openlattice.chronicle.services.studies.StudyLimitsManager
import com.openlattice.chronicle.services.studies.StudyService
import com.openlattice.chronicle.services.studies.tasks.StudyComplianceHazelcastTask
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.study.StudyComplianceApi
import com.openlattice.chronicle.study.StudyLimits
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
@RequestMapping(StudyComplianceApi.CONTROLLER)
class StudyComplianceController @Inject constructor(
    private val studyLimitsMgr: StudyLimitsManager,
    private val studyService: StudyService,
    private val studyComplianceManager: StudyComplianceManager,
    private val studyComplianceHazelcastTask: StudyComplianceHazelcastTask,
    private val storageResolver: StorageResolver,
    override val auditingManager: AuditingManager,
    override val authorizationManager: AuthorizationManager,
) : StudyComplianceApi, AuthorizingComponent {

    @GetMapping(
        value = [STUDY + STUDY_ID_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getStudyComplianceViolations(studyId: UUID): Map<UUID, Map<String, List<ComplianceViolation>>> {
        ensureReadAccess(AclKey(studyId))
        check(studyService.isValidStudy(studyId)) { "$studyId is not valid." }

        return studyComplianceManager.getNonCompliantStudies(listOf(studyId))
    }

    @PostMapping(
        value = [STUDY],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun triggerStudyComplianceNotifications(@RequestBody studyIds: Set<UUID>): OK {
        ensureAdminAccess()
        val nonCompliantStudies = studyComplianceManager.getNonCompliantStudies(studyIds)
        storageResolver.getPlatformStorage().connection.use { connection ->
            AuditedTransactionBuilder<Unit>(connection, auditingManager)
                .transaction {
                    studyComplianceHazelcastTask.notifyNonCompliantStudies(nonCompliantStudies)
                }.audit {
                    studyIds.map { studyId ->
                        AuditableEvent(
                            AclKey(studyId),
                            eventType = AuditEventType.TRIGGER_STUDY_COMPLIANCE_NOTIFICATIONS,
                            description = "Trigger study compliance notification job"
                        )
                    }
                }.buildAndRun()
        }
        return OK()
    }
}