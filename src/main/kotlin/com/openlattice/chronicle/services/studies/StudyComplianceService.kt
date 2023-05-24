package com.openlattice.chronicle.services.studies

import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.geekbeast.postgres.streams.StatementHolderSupplier
import com.hazelcast.core.HazelcastInstance
import com.openlattice.chronicle.auditing.*
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.notifications.DeliveryType
import com.openlattice.chronicle.notifications.NotificationType
import com.openlattice.chronicle.notifications.StudyNotificationSettings
import com.openlattice.chronicle.services.candidates.CandidateManager
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.services.notifications.NotificationService
import com.openlattice.chronicle.services.notifications.ResearcherNotification
import com.openlattice.chronicle.services.surveys.SurveysManager
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.STUDIES
import com.openlattice.chronicle.storage.PostgresColumns
import com.openlattice.chronicle.storage.RedshiftColumns
import com.openlattice.chronicle.storage.RedshiftDataTables
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.study.StudyDuration
import com.openlattice.chronicle.study.StudySettingType
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@getmethodic.com&gt;
 */
@Service
class StudyComplianceService(
    private val storageResolver: StorageResolver,
    private val authorizationService: AuthorizationManager,
    private val candidateService: CandidateManager,
    private val enrollmentService: EnrollmentManager,
    private val surveysManager: SurveysManager,
    private val idGenerationService: HazelcastIdGenerationService,
    private val studyLimitsMgr: StudyLimitsManager,
    private val studyService: StudyService,
    override val auditingManager: AuditingManager,
    hazelcast: HazelcastInstance,
) : StudyComplianceManager, AuditingComponent {
    private val studies = HazelcastMap.STUDIES.getMap(hazelcast)

    @Inject
    @org.springframework.context.annotation.Lazy
    private lateinit var notificationService: NotificationService

    companion object {
        private val logger = LoggerFactory.getLogger(StudyComplianceService::class.java)
        private val NOTIFICATION_ENABLED_STUDIES = """
            SELECT ${PostgresColumns.STUDY_ID} FROM ${STUDIES.name}
            WHERE ${PostgresColumns.NOTIFICATIONS_ENABLED.name} = true
        """.trimIndent()
        private val NO_DATA_STUDIES_SQL = """
            SELECT ${RedshiftColumns.STUDY_ID.name},${RedshiftColumns.PARTICIPANT_ID.name}
            FROM ${RedshiftDataTables.CHRONICLE_USAGE_EVENTS.name} 
            WHERE
        """
        private val NO_DATA_STUDIES_SUFFIX = """
            GROUP BY (${RedshiftColumns.STUDY_ID.name},${RedshiftColumns.PARTICIPANT_ID.name})
            HAVING count(*) = 0
        """.trimIndent()
    }

    fun getStudyParticipantsWithNoAndroidUploads() {
        val enabledStudies: List<UUID> = getStudiesWithNotificationsEnabled()
        if (enabledStudies.isEmpty()) {
            logger.info("No partcipants without data uploads in the specified timeframes.")
            return
        }
        val enabledStudiesSettings = studyService.getStudySettings(enabledStudies).filter { studySettings ->
            studySettings.value[StudySettingType.Notifications] != null
        }

        //Build the SQL to query all study participants that have not uploaded data within prescribed time.
        val sql = NO_DATA_STUDIES_SQL + enabledStudiesSettings.map {
            val notificationSettings = it.value.getValue(StudySettingType.Notifications) as StudyNotificationSettings
            toInterval(it.key, notificationSettings.noDataUploaded)
        }.joinToString(" AND ") + NO_DATA_STUDIES_SUFFIX


        val participantsByStudy =
            BasePostgresIterable(StatementHolderSupplier(storageResolver.getEventStorageWithFlavor(), sql)) { rs ->
                UUID.fromString(rs.getString(RedshiftColumns.STUDY_ID.name)) to rs.getString(RedshiftColumns.PARTICIPANT_ID.name)
            }.groupBy({ (studyId, _) -> studyId }, { (_, participantId) -> participantId })

        //For each study look up research contact email
        participantsByStudy.forEach { (studyId, participantIds) ->
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
                    (enabledStudiesSettings.getValue(studyId)
                        .getValue(StudySettingType.Notifications) as StudyNotificationSettings).noDataUploaded
                )
            )
            storageResolver.getPlatformStorage().connection.use { connection ->
                AuditedTransactionBuilder<Unit>(connection, auditingManager)
                    .transaction { conn ->
                        notificationService.sendResearcherNotifications(conn, studyId, listOf(researcherNotification))
                    }
                    .audit {
                        listOf(
                            AuditableEvent(
                                AclKey(studyId),
                                eventType = AuditEventType.QUEUE_NOTIFICATIONS,
                                description = "Queued $it notifications.",
                                study = studyId,
                            )
                        )
                    }
                    .buildAndRun()
            }

        }

    }

    private fun getStudiesWithNotificationsEnabled(): List<UUID> {
        return BasePostgresIterable(PreparedStatementHolderSupplier(
            storageResolver.getPlatformStorage(),
            NOTIFICATION_ENABLED_STUDIES, 1024
        ) {}) { rs ->
            rs.getObject(PostgresColumns.STUDY_ID.name, UUID::class.java)
        }.toList()
    }

    private fun buildMessage(studyId: UUID, studyTitle: String, studyDuration: StudyDuration): String {
        var msg = """
            The following participants in $studyTitle ($studyId) have not uploaded any data in the last 
        """.trimIndent()

        if (studyDuration.years > 0) {
            msg += "${studyDuration.years} years "
        }

        if (studyDuration.months > 0) {
            msg += "${studyDuration.months} months "
        }

        if (studyDuration.days > 0) {
            msg += "${studyDuration.days} days "
        }
        return msg
    }

    private fun toInterval(studyId: UUID, studyDuration: StudyDuration): String {
        return """
            (${RedshiftColumns.STUDY_ID.name} = $studyId AND ${RedshiftColumns.TIMESTAMP.name} > '${studyDuration.years} years ${studyDuration.months} months ${studyDuration.days} days'::interval)
        """.trimIndent()
    }
}