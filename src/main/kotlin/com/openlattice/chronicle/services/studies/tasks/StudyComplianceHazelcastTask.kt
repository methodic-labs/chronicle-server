package com.openlattice.chronicle.services.studies.tasks

import com.geekbeast.tasks.HazelcastFixedRateTask
import com.geekbeast.tasks.HazelcastTaskDependencies
import com.geekbeast.tasks.Task
import com.geekbeast.util.StopWatch
import com.openlattice.chronicle.authorization.principals.Principals
import com.openlattice.chronicle.notifications.DeliveryType
import com.openlattice.chronicle.notifications.NotificationType
import com.openlattice.chronicle.notifications.StudyNotificationSettings
import com.openlattice.chronicle.services.notifications.NotificationManager
import com.openlattice.chronicle.services.notifications.ResearcherNotification
import com.openlattice.chronicle.study.ComplianceViolation
import com.openlattice.chronicle.study.StudyComplianceManager
import com.openlattice.chronicle.services.studies.StudyManager
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.study.StudySettingType
import org.slf4j.LoggerFactory
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.TimeUnit

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@getmethodic.com&gt;
 */
class StudyComplianceHazelcastTask : HazelcastFixedRateTask<StudyComplianceHazelcastTaskDependencies> {
    companion object {
        private val logger = LoggerFactory.getLogger(StudyComplianceHazelcastTask::class.java)
        private val zoneIds = listOf("America/New_York", "Europe/Berlin")
    }

    override fun getInitialDelay(): Long {
        return zoneIds.maxOf { zoneId ->
            //What time is it in the desired zoneId
            val current = OffsetDateTime.now().atZoneSameInstant(ZoneId.of(zoneId))
            val currentTime = current.toOffsetDateTime().toOffsetTime()
            //9 AM in the desired time zone
            val targetNextRunTime = LocalTime.of(9, 0).atOffset(current.offset)
            val targetNextRunDateTime = targetNextRunTime.atDate(current.toLocalDate())

            ChronoUnit.MINUTES.between(
                current,
                if (currentTime > targetNextRunTime) targetNextRunDateTime.plusDays(1) else targetNextRunDateTime
            )
        }
    }

    override fun getPeriod(): Long = 12 * 60

    override fun getTimeUnit(): TimeUnit = TimeUnit.MINUTES

    override fun runTask() {
        logger.info("Running study compliance task.")
        StopWatch("Processing notification for non-compliant study participants").run {
            val nonCompliantStudies = getDependency().studyComplianceManager.getAllNonCompliantStudies()
            notifyNonCompliantStudies(nonCompliantStudies)
        }
    }

    fun notifyNonCompliantStudies(nonCompliantStudies: Map<UUID, Map<String, List<ComplianceViolation>>>) {
        val studyService = getDependency().studyService
        val storageResolver = getDependency().storageResolver
        val notificationService = getDependency().notificationService
        //For each study look up research contact email
        nonCompliantStudies.forEach { (studyId, participantViolations) ->
            logger.info("Sending non-compliance notification for study $studyId: $participantViolations")
            val study = studyService.getStudy(studyId)
            val studyEmails = study.contact.split(",").toSet()
            val phoneNumbers = (study.settings[StudySettingType.Notifications] as StudyNotificationSettings?)
                ?.researcherPhoneNumbers?.split(",")?.toSet() ?: emptySet()
            val researcherNotification = ResearcherNotification(
                studyEmails,
                phoneNumbers,
                NotificationType.PASSIVE_DATA_COLLECTION_COMPLIANCE,
                EnumSet.of(DeliveryType.EMAIL),
                "Compliance violations for ${study.title} ($studyId)",
                buildMessage(
                    studyId,
                    study.title,
                    participantViolations
                )
            )
            storageResolver.getPlatformStorage().connection.use { connection ->
                try {
                    connection.autoCommit = false
                    notificationService.sendResearcherNotifications(
                        connection,
                        studyId,
                        listOf(researcherNotification),
                        true,
                        Principals.getMethodicPrincipal()
                    )
                    connection.commit()
                    connection.autoCommit = true
                } catch (ex: Exception) {
                    logger.info("Unable to send notifications.", ex)
                    connection.rollback()
                }
            }
        }
    }

    private fun buildMessage(
        studyId: UUID,
        studyTitle: String,
        participantViolations: Map<String, List<ComplianceViolation>>,
    ): String {
        val violationTableRows = participantViolations.flatMap { (participantId, violations) ->
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
            The following compliance violations where found for $studyTitle ($studyId).
            <table role = "presentation" border="1" width="75%">
            <tr>
            <th> Participant ID </th>
            <th> Violation </th>
            <th> Description </th>
            </tr>
            $violationTableRows
            </table>
        """.trimIndent()

    }

    override fun getName(): String = Task.STUDY_COMPLIANCE_TASK.name

    override fun getDependenciesClass(): Class<out StudyComplianceHazelcastTaskDependencies> =
        StudyComplianceHazelcastTaskDependencies::class.java
}

data class StudyComplianceHazelcastTaskDependencies(
    val studyComplianceManager: StudyComplianceManager,
    val studyService: StudyManager,
    val storageResolver: StorageResolver,
    val notificationService: NotificationManager,
) : HazelcastTaskDependencies
