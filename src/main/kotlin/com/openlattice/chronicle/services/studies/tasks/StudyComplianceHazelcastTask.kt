package com.openlattice.chronicle.services.studies.tasks

import com.geekbeast.tasks.HazelcastFixedRateTask
import com.geekbeast.tasks.HazelcastTaskDependencies
import com.openlattice.chronicle.auditing.AuditEventType
import com.openlattice.chronicle.auditing.AuditableEvent
import com.openlattice.chronicle.auditing.AuditedTransactionBuilder
import com.openlattice.chronicle.notifications.DeliveryType
import com.openlattice.chronicle.notifications.NotificationType
import com.openlattice.chronicle.notifications.StudyNotificationSettings
import com.openlattice.chronicle.services.notifications.NotificationManager
import com.openlattice.chronicle.services.notifications.NotificationService
import com.openlattice.chronicle.services.notifications.ResearcherNotification
import com.openlattice.chronicle.services.studies.ComplianceViolation
import com.openlattice.chronicle.services.studies.StudyComplianceManager
import com.openlattice.chronicle.services.studies.StudyManager
import com.openlattice.chronicle.services.studies.StudyService
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.study.StudyDuration
import com.openlattice.chronicle.study.StudySettingType
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.TimeUnit

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@getmethodic.com&gt;
 */
class StudyComplianceHazelcastTask : HazelcastFixedRateTask<StudyComplianceHazelcastTaskDependencies> {
    companion object {
        private val logger = LoggerFactory.getLogger(StudyComplianceHazelcastTask::class.java)
    }

    override fun getInitialDelay(): Long = 0

    override fun getPeriod(): Long = 15

    override fun getTimeUnit(): TimeUnit = TimeUnit.MINUTES

    override fun runTask() {
        logger.info("Running study compliance task.")
        val nonCompliantStudies = getDependency().studyComplianceManager.getAllNonCompliantStudies()
        val studyService = getDependency().studyService
        val storageResolver = getDependency().storageResolver
        val notificationService = getDependency().notificationService
        //For each study look up research contact email
        nonCompliantStudies.forEach { (studyId, participantViolations) ->
            val study = studyService.getStudy(studyId)
            val studyEmails = study.contact.split(",").toSet()
            val phoneNumbers = study.phoneNumber.split(",").toSet()
            val researcherNotification = ResearcherNotification(
                studyEmails,
                phoneNumbers,
                NotificationType.PASSIVE_DATA_COLLECTION_COMPLIANCE,
                EnumSet.of(DeliveryType.EMAIL, DeliveryType.SMS),
                buildMessage(
                    studyId,
                    study.title,
                    participantViolations
                )
            )
            storageResolver.getPlatformStorage().connection.use { connection ->
                notificationService.sendResearcherNotifications(
                    connection,
                    studyId,
                    listOf(researcherNotification),
                    true
                )
            }
        }
    }

    private fun buildMessage(
        studyId: UUID,
        studyTitle: String,
        participantViolations: Map<String, List<ComplianceViolation>>
    ): String {
        val violationTableRows = participantViolations.map { (participantId, violations) ->
            violations.map { violation ->
                """
                <tr>
                <td>${participantId}</td>
                <td>${violation.reason}</td>
                <td>${violation.description}</td>
                </tr>
            """.trimMargin()
            }
        }.joinToString("\n")
        return """
            <style>
            table, th, td {
              border:1px solid black;
            }
            </style>
            The following compliance violations where found for $studyTitle ($studyId).
            <table>
            <tr>
            <th> Participant ID </th>
            <th> Violation </th>
            <th> Description </th>
            </tr>
            $violationTableRows
            </table>
        """.trimIndent()

    }

    override fun getName(): String = "study_compliance_task"

    override fun getDependenciesClass(): Class<out StudyComplianceHazelcastTaskDependencies> =
        StudyComplianceHazelcastTaskDependencies::class.java
}

data class StudyComplianceHazelcastTaskDependencies(
    val studyComplianceManager: StudyComplianceManager,
    val studyService: StudyManager,
    val storageResolver: StorageResolver,
    val notificationService: NotificationManager
) : HazelcastTaskDependencies
