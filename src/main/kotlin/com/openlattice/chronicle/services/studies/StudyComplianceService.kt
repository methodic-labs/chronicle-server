package com.openlattice.chronicle.services.studies

import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.geekbeast.postgres.streams.StatementHolderSupplier
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicates
import com.openlattice.chronicle.auditing.*
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.mapstores.storage.StudyMapstore.Companion.NOTIFY_RESEARCHERS_INDEX
import com.openlattice.chronicle.notifications.DeliveryType
import com.openlattice.chronicle.notifications.NotificationType
import com.openlattice.chronicle.notifications.StudyNotificationSettings
import com.openlattice.chronicle.services.candidates.CandidateManager
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.services.notifications.NotificationManager
import com.openlattice.chronicle.services.notifications.NotificationService
import com.openlattice.chronicle.services.notifications.ResearcherNotification
import com.openlattice.chronicle.services.surveys.SurveysManager
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.STUDIES
import com.openlattice.chronicle.storage.PostgresColumns
import com.openlattice.chronicle.storage.RedshiftColumns
import com.openlattice.chronicle.storage.RedshiftDataTables
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.study.Study
import com.openlattice.chronicle.study.StudyDuration
import com.openlattice.chronicle.study.StudySetting
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
    override val auditingManager: AuditingManager,
    hazelcast: HazelcastInstance,
) : StudyComplianceManager, AuditingComponent {
    private val studies = HazelcastMap.STUDIES.getMap(hazelcast)

    companion object {
        private val logger = LoggerFactory.getLogger(StudyComplianceService::class.java)

        //Will retrieve all studies that notifications enabled.
        private val ALL_NOTIFICATION_ENABLED_STUDIES = """
            SELECT ${PostgresColumns.STUDY_ID.name} FROM ${STUDIES.name}
            WHERE COALESCE(${PostgresColumns.SETTINGS.name}->'Notifications'->'researchNotificationsEnabled','true')::bool = true
        """.trimIndent()

        private val NOTIFICATION_ENABLED_STUDIES = """
            $ALL_NOTIFICATION_ENABLED_STUDIES AND ${PostgresColumns.STUDY_ID.name} = ANY(?)
        """.trimIndent()

        val NO_DATA_STUDIES_SUFFIX = """
            GROUP BY (${RedshiftColumns.STUDY_ID.name},${RedshiftColumns.PARTICIPANT_ID.name})
            HAVING count(*) = 0
        """.trimIndent()

        fun getNoDataUploadSql(dataTable: String): String {
            return """
            SELECT ${RedshiftColumns.STUDY_ID.name},${RedshiftColumns.PARTICIPANT_ID.name}
            FROM $dataTable
            WHERE
        """.trimIndent()
        }

        private fun getIntervalSql(studyId: UUID, studyDuration: StudyDuration, timestampColumn: String): String {
            return """
            (${RedshiftColumns.STUDY_ID.name} = $studyId AND $timestampColumn >= '${studyDuration.years} years ${studyDuration.months} months ${studyDuration.days} days'::interval)
        """.trimIndent()
        }

        private fun buildSql(
            dataTable: String,
            timestampColumn: String,
            enabledStudiesSettings: Map<UUID, Map<StudySettingType, StudySetting>>,
        ): String {
            //Build the SQL to query all study participants that have not uploaded data within prescribed time.
            return getNoDataUploadSql(dataTable) + enabledStudiesSettings.map {
                val notificationSettings =
                    it.value.getValue(StudySettingType.Notifications) as StudyNotificationSettings
                getIntervalSql(it.key, notificationSettings.noDataUploaded, timestampColumn)
            }.joinToString(" AND ") + NO_DATA_STUDIES_SUFFIX

        }

        fun getDurationPolicy(
            studyId: UUID,
            enabledStudiesSettings: Map<UUID, Map<StudySettingType, StudySetting>>
        ): StudyDuration {
            return (enabledStudiesSettings
                .getValue(studyId)
                .getValue(StudySettingType.Notifications) as StudyNotificationSettings).noDataUploaded
        }

    }

    override fun getNonCompliantStudies(studies: Collection<UUID>): Map<UUID, Map<String, List<ComplianceViolation>>> {
        return if (studies.isEmpty()) mapOf() else getNonCompliantStudiesById(studies)

    }

    override fun getAllNonCompliantStudies(): Map<UUID, Map<String, List<ComplianceViolation>>> {
        return getNonCompliantStudiesById(emptyList())
    }

    private fun getNonCompliantStudiesById(studies: Collection<UUID>): Map<UUID, Map<String, List<ComplianceViolation>>> {
        //Studies being empty in this function means that it will return all studies.
        val enabledStudiesSettings = getStudiesWithNotificationsEnabled(studies)
        if (enabledStudiesSettings.isEmpty()) {
            logger.info("No partcipants without data uploads in the specified timeframes.")
            return mapOf()
        }

        val androidUploadViolations = getStudyParticipantsWithoutUploads(
            buildSql(
                RedshiftDataTables.CHRONICLE_USAGE_EVENTS.name,
                RedshiftColumns.TIMESTAMP.name,
                enabledStudiesSettings
            ), enabledStudiesSettings
        )

        val iosUploadViolations = getStudyParticipantsWithoutUploads(
            buildSql(
                RedshiftDataTables.IOS_SENSOR_DATA.name,
                RedshiftColumns.RECORDED_DATE.name,
                enabledStudiesSettings
            ),
            enabledStudiesSettings
        )

        //In the future we can add additional compliance checks above and below.
        return (androidUploadViolations.asSequence() + iosUploadViolations.asSequence())
            .groupBy({ it.key }, { it.value })
            .mapValues { participantViolations ->
                participantViolations.value.flatten().groupBy({ it.first }, { it.second })
            }


    }

    private fun getStudyParticipantsWithoutUploads(
        sql: String,
        enabledStudiesSettings: Map<UUID, Map<StudySettingType, StudySetting>>,
    ): Map<UUID, List<Pair<String, ComplianceViolation>>> {
        return BasePostgresIterable(StatementHolderSupplier(storageResolver.getEventStorageWithFlavor(), sql)) { rs ->
            UUID.fromString(rs.getString(RedshiftColumns.STUDY_ID.name)) to rs.getString(RedshiftColumns.PARTICIPANT_ID.name)
        }.groupBy({ (studyId, _) -> studyId }) { (studyId, participantId) ->
            val violation = ComplianceViolation(
                ViolationReason.NO_DATA_UPLOADED,
                buildDescription(studyId, getDurationPolicy(studyId, enabledStudiesSettings))
            )
            participantId to violation
        }
    }


    private fun buildDescription(studyId: UUID, studyDuration: StudyDuration): String {
        return """
            Study policy for $studyId requires uploads within the last ${studyDuration.years} years ${studyDuration.months} months ${studyDuration.days} days.
        """.trimIndent()
    }

    private fun getStudiesWithNotificationsEnabled(studyIds: Collection<UUID>): Map<UUID, Map<StudySettingType, StudySetting>> {
        return studies
            .values(
                if (studyIds.isEmpty()) {
                    Predicates.equal(NOTIFY_RESEARCHERS_INDEX, true)
                } else {
                    Predicates.and(
                        Predicates.`in`<UUID, Study>(NOTIFY_RESEARCHERS_INDEX, *studyIds.toTypedArray()),
                        Predicates.equal<UUID, Study>(NOTIFY_RESEARCHERS_INDEX, true)
                    )
                }
            )
            .associateBy({ it.id }, { it.settings })
//        return studyService.getStudySettings(
//            BasePostgresIterable(
//                PreparedStatementHolderSupplier(
//                    storageResolver.getPlatformStorage(),
//                    NOTIFICATION_ENABLED_STUDIES,
//                    1024
//                ) { ps -> ps.setArray(1, PostgresArrays.createUuidArray(ps.connection, studyIds)) }
//            ) { rs -> rs.getObject(PostgresColumns.STUDY_ID.name, UUID::class.java) }.toList()
//        ).filter { studySettings ->
//            studySettings.value[StudySettingType.Notifications] != null
//        }
    }


}