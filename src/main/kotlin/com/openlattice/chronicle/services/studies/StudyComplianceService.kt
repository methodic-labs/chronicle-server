package com.openlattice.chronicle.services.studies

import com.geekbeast.configuration.postgres.PostgresFlavor
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.StatementHolderSupplier
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicates
import com.openlattice.chronicle.auditing.*
import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.openlattice.chronicle.mapstores.storage.StudyMapstore.Companion.NOTIFY_RESEARCHERS_INDEX
import com.openlattice.chronicle.notifications.StudyNotificationSettings
import com.openlattice.chronicle.storage.*
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.STUDIES
import com.openlattice.chronicle.study.*
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*

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


        private val ACTIVE_PARTICIPANTS = """
            SELECT ${PostgresColumns.STUDY_ID.name}, ${PostgresColumns.PARTICIPANT_ID.name}
            FROM ${ChroniclePostgresTables.STUDY_PARTICIPANTS.name}
            WHERE ${PostgresColumns.PARTICIPATION_STATUS.name} = '${ParticipationStatus.ENROLLED}'
        """.trimIndent()

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
            HAVING count(*) > 0
        """.trimIndent()

        fun getDataUploadedSql(dataTable: String): String {
            return """
            SELECT ${RedshiftColumns.STUDY_ID.name},${RedshiftColumns.PARTICIPANT_ID.name}
            FROM $dataTable
            WHERE
        """.trimIndent()
        }

        private fun getIntervalSql(
            studyId: UUID,
            studyDuration: StudyDuration,
            timestampColumn: String,
            flavor: PostgresFlavor,
        ): String {
            val nowFunction = when (flavor) {
                PostgresFlavor.REDSHIFT -> "GETDATE()"
                PostgresFlavor.VANILLA -> "now()"
                else -> "now()"
            }
            return """
            (${RedshiftColumns.STUDY_ID.name} = '$studyId' AND $timestampColumn >= $nowFunction - '${studyDuration.years} years ${studyDuration.months} months ${studyDuration.days} days'::interval) 
        """.trimIndent()
        }

        private fun buildSql(
            dataTable: String,
            timestampColumn: String,
            enabledStudiesSettings: Map<UUID, Map<StudySettingType, StudySetting>>,
            filterByInterval: Boolean = true,
            flavor: PostgresFlavor,
        ): String {
            //Build the SQL to query all study participants that have not uploaded data within prescribed time.
            return getDataUploadedSql(dataTable) + enabledStudiesSettings.map { (studyId, studySettings) ->
                val notificationSettings =
                    studySettings.getValue(StudySettingType.Notifications) as StudyNotificationSettings
                if (filterByInterval) {
                    getIntervalSql(studyId, notificationSettings.noDataUploaded, timestampColumn, flavor)
                } else {
                    "${RedshiftColumns.STUDY_ID.name} = '$studyId'"
                }
            }.joinToString(" OR ") + NO_DATA_STUDIES_SUFFIX

        }

        fun getDurationPolicy(
            studyId: UUID,
            enabledStudiesSettings: Map<UUID, Map<StudySettingType, StudySetting>>,
        ): StudyDuration {
            return (enabledStudiesSettings
                .getValue(studyId)
                .getValue(StudySettingType.Notifications) as StudyNotificationSettings).noDataUploaded
        }
    }

    override fun getNonCompliantStudies(studies: Collection<UUID>): Map<UUID, Map<String, List<ComplianceViolation>>> {
        return if (studies.isEmpty()) mapOf() else getNonCompliantParticipantsByStudy(studies)

    }

    override fun getAllNonCompliantStudies(): Map<UUID, Map<String, List<ComplianceViolation>>> {
        return getNonCompliantParticipantsByStudy(emptyList())
    }

    private fun getNonCompliantParticipantsByStudy(studies: Collection<UUID>): Map<UUID, Map<String, List<ComplianceViolation>>> {
        logger.info("Getting non-compliant participants for the following studies: $studies")
        //Studies being empty in this function means that it will return all studies.
        val enabledStudiesSettings = getStudiesWithNotificationsEnabled(studies)
        if (enabledStudiesSettings.isEmpty()) {
            logger.info("No partcipants without data uploads in the specified timeframes.")
            return mapOf()
        }

        val activeParticipants = getActiveStudyParticipants(enabledStudiesSettings.keys)


        //One thing that will break here is that we are grouping studies by their flavor so if we ever store some studies
        //on postgres and some on redshift, we will need to fix that logic here.
        val androidUploadViolations = getStudyParticipantsWithoutRecentUploads(
            buildSql(
                RedshiftDataTables.CHRONICLE_USAGE_EVENTS.name,
                RedshiftColumns.TIMESTAMP.name,
                enabledStudiesSettings,
                true,
                storageResolver.getDefaultEventStorage().first
            ), enabledStudiesSettings,
            activeParticipants
        )

        val iosUploadViolations = getStudyParticipantsWithoutRecentUploads(
            buildSql(
                RedshiftDataTables.IOS_SENSOR_DATA.name,
                RedshiftColumns.RECORDED_DATE_TIME.name,
                enabledStudiesSettings,
                true,
                storageResolver.getDefaultEventStorage().first
            ),
            enabledStudiesSettings,
            activeParticipants
        )

        //In the future we can add additional compliance checks above and below.
        return (androidUploadViolations.asSequence() + iosUploadViolations.asSequence())
            .groupBy({ it.key }, { it.value })
            .mapValues { participantViolations ->
                participantViolations.value.flatten().groupBy({ it.first }, { it.second })
            }


    }

    private fun getStudyParticipantsWithUploads(
        sql: String,
        enabledStudiesSettings: Map<UUID, Map<StudySettingType, StudySetting>>,
    ): Map<UUID, List<Pair<String, ComplianceViolation>>> {
        return BasePostgresIterable(
            StatementHolderSupplier(
                storageResolver.getDefaultEventStorage().second,
                sql
            )
        ) { rs ->
            UUID.fromString(rs.getString(RedshiftColumns.STUDY_ID.name)) to rs.getString(RedshiftColumns.PARTICIPANT_ID.name)
        }.groupBy({ (studyId, _) -> studyId }) { (studyId, participantId) ->
            val violation = ComplianceViolation(
                ViolationReason.NO_DATA_UPLOADED,
                buildDescription(studyId, getDurationPolicy(studyId, enabledStudiesSettings))
            )
            participantId to violation
        }
    }

    private fun getStudyParticipantsWithoutRecentUploads(
        sql: String,
        enabledStudiesSettings: Map<UUID, Map<StudySettingType, StudySetting>>,
        activeParticipants: Map<UUID, Set<String>>,
    ): Map<UUID, List<Pair<String, ComplianceViolation>>> {
        val studyParticipants = mutableMapOf<UUID, MutableSet<String>>()
        //Load all participants with recent uploads.
        BasePostgresIterable(
            StatementHolderSupplier(
                storageResolver.getDefaultEventStorage().second,
                sql
            )
        ) { rs ->
            UUID.fromString(rs.getString(RedshiftColumns.STUDY_ID.name)) to rs.getString(RedshiftColumns.PARTICIPANT_ID.name)
        }
            .forEach { (studyId, participantId) ->
                studyParticipants.getOrPut(studyId) { mutableSetOf() }.add(participantId)
            }

        //Take all active participants and remove any who have recently uploaded data.
        return activeParticipants.mapValues { (studyId, participantIds) ->
            val violation = ComplianceViolation(
                ViolationReason.NO_RECENT_DATA_UPLOADED,
                buildDescription(studyId, getDurationPolicy(studyId, enabledStudiesSettings))
            )

            (participantIds - (studyParticipants[studyId] ?: emptySet()).toSet())
                .map { it to violation }

        }
    }

    private fun getActiveStudyParticipants(studyFilter: Set<UUID>): Map<UUID, Set<String>> {
        val studyParticipants = mutableMapOf<UUID, MutableSet<String>>()
        BasePostgresIterable(
            StatementHolderSupplier(
                storageResolver.getPlatformStorage(),
                ACTIVE_PARTICIPANTS,
                1024
            )
        ) { rs ->
            rs.getObject(
                PostgresColumns.STUDY_ID.name,
                UUID::class.java
            ) to rs.getString(PostgresColumns.PARTICIPANT_ID.name)
        }
            .asSequence()
            .filter { studyFilter.contains(it.first) }
            .forEach { (studyId, participantId) ->
                studyParticipants.getOrPut(studyId) { mutableSetOf() }.add(participantId)
            }
        return studyParticipants
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
                        Predicates.`in`<UUID, Study>("__key", *studyIds.toTypedArray()),
                        Predicates.equal<UUID, Study>(NOTIFY_RESEARCHERS_INDEX, true)
                    )
                }
            )
            .filter { it.settings.containsKey(StudySettingType.Notifications) }
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