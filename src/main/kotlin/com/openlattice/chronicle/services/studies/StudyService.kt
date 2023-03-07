package com.openlattice.chronicle.services.studies

import com.fasterxml.jackson.module.kotlin.readValue
import com.geekbeast.configuration.postgres.PostgresFlavor
import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.PostgresDatatype
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.hazelcast.core.HazelcastInstance
import com.openlattice.chronicle.auditing.*
import com.openlattice.chronicle.authorization.*
import com.openlattice.chronicle.authorization.Permission.READ
import com.openlattice.chronicle.authorization.principals.Principals
import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.ids.IdConstants
import com.openlattice.chronicle.notifications.DeliveryType
import com.openlattice.chronicle.notifications.NotificationType
import com.openlattice.chronicle.notifications.ParticipantNotification
import com.openlattice.chronicle.notifications.StudyNotificationSettings
import com.openlattice.chronicle.participants.Participant
import com.openlattice.chronicle.participants.ParticipantStats
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.sensorkit.SensorSetting
import com.openlattice.chronicle.sensorkit.SensorType
import com.openlattice.chronicle.services.candidates.CandidateManager
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.services.notifications.NotificationService
import com.openlattice.chronicle.services.studies.processors.StudyPhoneNumberGetter
import com.openlattice.chronicle.services.surveys.SurveysManager
import com.openlattice.chronicle.sources.AndroidDevice
import com.openlattice.chronicle.sources.IOSDevice
import com.openlattice.chronicle.sources.SourceDevice
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.LEGACY_STUDY_IDS
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.ORGANIZATION_STUDIES
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.PARTICIPANT_STATS
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.PERMISSIONS
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.STUDIES
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.STUDY_PARTICIPANTS
import com.openlattice.chronicle.storage.PostgresColumns
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ACL_KEY
import com.openlattice.chronicle.storage.PostgresColumns.Companion.CONTACT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.CREATED_AT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.DESCRIPTION
import com.openlattice.chronicle.storage.PostgresColumns.Companion.LAT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.LEGACY_STUDY_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.LON
import com.openlattice.chronicle.storage.PostgresColumns.Companion.MODULES
import com.openlattice.chronicle.storage.PostgresColumns.Companion.NOTIFICATIONS_ENABLED
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ORGANIZATION_IDS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PARTICIPATION_STATUS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PRINCIPAL_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SETTINGS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STORAGE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_GROUP
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_PHONE_NUMBER
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_VERSION
import com.openlattice.chronicle.storage.PostgresColumns.Companion.TITLE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.UPDATED_AT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.USER_ID
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.study.Study
import com.openlattice.chronicle.study.StudySetting
import com.openlattice.chronicle.study.StudySettingType
import com.openlattice.chronicle.study.StudyUpdate
import com.openlattice.chronicle.util.ChronicleServerUtil
import com.openlattice.chronicle.util.ensureVanilla
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.sql.Connection
import java.time.OffsetDateTime
import java.util.*
import javax.inject.Inject

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
@Service
class StudyService(
    private val storageResolver: StorageResolver,
    private val authorizationService: AuthorizationManager,
    private val candidateService: CandidateManager,
    private val enrollmentService: EnrollmentManager,
    private val surveysManager: SurveysManager,
    private val idGenerationService: HazelcastIdGenerationService,
    private val studyLimitsMgr: StudyLimitsManager,
    override val auditingManager: AuditingManager,
    hazelcast: HazelcastInstance,
) : StudyManager, AuditingComponent {
    private val studies = HazelcastMap.STUDIES.getMap(hazelcast)

    @Inject
    @org.springframework.context.annotation.Lazy
    private lateinit var notificationService: NotificationService

    companion object {
        private val logger = LoggerFactory.getLogger(StudyService::class.java)
        private val mapper = ObjectMappers.newJsonMapper()
        private val STUDY_PHONE_NUMBER_GETTER = StudyPhoneNumberGetter()
        private val STUDY_COLUMNS_LIST = listOf(
            STUDY_ID,
            TITLE,
            DESCRIPTION,
            LAT,
            LON,
            STUDY_GROUP,
            STUDY_VERSION,
            CONTACT,
            NOTIFICATIONS_ENABLED,
            STORAGE,
            SETTINGS,
            MODULES,
            STUDY_PHONE_NUMBER
        )
        private val STUDY_COLUMNS = STUDY_COLUMNS_LIST.joinToString(",") { it.name }
        private val STUDY_COLUMNS_BIND = STUDY_COLUMNS_LIST.joinToString(",") {
            if (it.datatype == PostgresDatatype.JSONB) "?::jsonb" else "?"
        }

        private val UPDATE_STUDY_COLUMNS_LIST = listOf(
            TITLE,
            DESCRIPTION,
            UPDATED_AT,
            LAT,
            LON,
            STUDY_GROUP,
            STUDY_VERSION,
            CONTACT,
            NOTIFICATIONS_ENABLED,
            STORAGE,
            SETTINGS,
            MODULES
        )

        private val UPDATE_STUDY_COLUMNS = UPDATE_STUDY_COLUMNS_LIST.joinToString(",") { it.name }
        private val COALESCED_STUDY_COLUMNS_BIND = UPDATE_STUDY_COLUMNS_LIST
            .joinToString(",") {
                "coalesce(?${if (it.datatype == PostgresDatatype.JSONB) "::jsonb" else ""}, ${it.name})"
            }

        private val ORG_STUDIES_COLS = listOf(
            ORGANIZATION_ID,
            STUDY_ID,
            USER_ID,
            SETTINGS
        ).joinToString(",") { it.name }

        /**
         * 1. study id
         * 2. title
         * 3. description
         * 4. lat
         * 5. lon
         * 6. study group
         * 7. study version
         * 8. contact
         * 9. notifications enabled
         * 10. storage
         * 11. settings
         */
        private val INSERT_STUDY_SQL = """
            INSERT INTO ${STUDIES.name} ($STUDY_COLUMNS) VALUES ($STUDY_COLUMNS_BIND)
        """.trimIndent()

        /**
         * 1. organization_id
         * 2. study_id
         * 3. user_id
         * 4. settings
         */
        private val INSERT_ORG_STUDIES_SQL =
            """
                INSERT INTO ${ORGANIZATION_STUDIES.name} (${ORG_STUDIES_COLS}) VALUES (?,?,?,?::jsonb)
            """.trimIndent()

        internal val GET_STUDIES_SQL = """
            SELECT * FROM ${STUDIES.name} 
            LEFT JOIN (
                SELECT ${STUDY_ID.name}, array_agg(${ORGANIZATION_ID.name}) as ${ORGANIZATION_IDS.name} 
                    FROM ${ORGANIZATION_STUDIES.name}
                    GROUP BY ${STUDY_ID.name}
                ) as org_studies 
            USING (${STUDY_ID.name}) 
            WHERE ${STUDY_ID.name} = ANY(?)
        """.trimIndent()

        private val DELETE_STUDIES_SQL = """
            DELETE FROM ${STUDIES.name} WHERE ${STUDY_ID.name} = ANY(?)
        """.trimIndent()

        private val REMOVE_STUDIES_FROM_ORGS_SQL = """
            DELETE FROM ${ORGANIZATION_STUDIES.name} WHERE ${STUDY_ID.name} = ANY(?)
        """.trimIndent()

        private val REMOVE_ALL_PARTICIPANTS_FROM_STUDIES_SQL = """
            DELETE FROM ${STUDY_PARTICIPANTS.name} WHERE ${STUDY_ID.name} = ANY(?)
        """.trimIndent()

        private val REMOVE_PARTICIPANTS_FROM_STUDY_SQL = """
            DELETE FROM ${STUDY_PARTICIPANTS.name}
            WHERE ${STUDY_ID.name} = ?
            AND ${PARTICIPANT_ID.name} = ANY(?)
        """.trimIndent()

        /**
         * 1. title,
         * 2. description,
         * 3. updated_at
         * 4. lat,
         * 5. lon,
         * 6. study_group,
         * 7. study_version,
         * 8. contact,
         * 9. notifications enabled
         * 10. storage
         * 11. settings
         * 12. modules
         * 13. study_id
         */

        private val UPDATE_STUDY_SQL = """
            UPDATE ${STUDIES.name}
            SET (${UPDATE_STUDY_COLUMNS}) = (${COALESCED_STUDY_COLUMNS_BIND})
            WHERE ${STUDY_ID.name} = ?
        """.trimIndent()

        /**
         * TODO: Retrieve
         */
        private val GET_ORGANIZATION_ID = """
            SELECT ${ORGANIZATION_ID.name} FROM ${ORGANIZATION_STUDIES.name} WHERE ${STUDY_ID.name} = ? LIMIT 1 
        """.trimIndent()

        private val GET_NOTIFICATION_STATUS_SQL = """
            SELECT ${NOTIFICATIONS_ENABLED.name} FROM ${STUDIES.name} WHERE ${STUDY_ID.name} = ?
        """

        /**
         * get studies that belong to the provided organizationId where the
         * current user has read access
         * include list of all organizationIds each study is a part of.
         * sort by most recently created
         * 1. current user principal id
         * 2. organization id
         */
        private val GET_ORG_STUDIES_SQL = """
            SELECT ${STUDIES.name}.*, org_studies.${ORGANIZATION_IDS.name}
            FROM ${STUDIES.name}
                INNER JOIN ${PERMISSIONS.name}
                ON ${STUDIES.name}.${STUDY_ID.name} = ${PERMISSIONS.name}.${ACL_KEY.name}[1]
                    AND ${PRINCIPAL_ID.name} = ?
                    AND '${READ.name}' = ANY(${PostgresColumns.PERMISSIONS.name})
                LEFT JOIN (
                    SELECT ${STUDY_ID.name}, array_agg(${ORGANIZATION_ID.name}) as ${ORGANIZATION_IDS.name} 
                        FROM ${ORGANIZATION_STUDIES.name}
                        GROUP BY ${STUDY_ID.name}
                    ) as org_studies
                USING (${STUDY_ID.name}) 
            WHERE ${STUDY_ID.name}
            IN (
                SELECT ${STUDY_ID.name} FROM ${ORGANIZATION_STUDIES.name}
                WHERE ${ORGANIZATION_ID.name} = ?
            )
            ORDER BY ${CREATED_AT.name} DESC
        """.trimIndent()


        /**
         * PreparedStatement bind order:
         * 1) StudyId
         */
        private val GET_STUDY_SETTINGS_SQL = """
            SELECT ${SETTINGS.name}
            FROM ${STUDIES.name}
            WHERE ${STUDY_ID.name} = ?
        """.trimIndent()

        private val SELECT_STUDY_PARTICIPANTS_SQL = """
            SELECT * FROM ${STUDY_PARTICIPANTS.name} WHERE ${STUDY_ID.name} = ?
        """.trimIndent()

        private fun selectStudyIdSql(table: String): String {
            return when (table) {
                LEGACY_STUDY_IDS.name -> """
                    SELECT ${STUDY_ID.name} FROM ${LEGACY_STUDY_IDS.name} WHERE ${LEGACY_STUDY_ID.name} = ?                    
                """.trimIndent()
                STUDIES.name -> """
                    SELECT ${STUDY_ID.name} FROM ${STUDIES.name} WHERE ${STUDY_ID.name} = ?
                """.trimIndent()
                else -> ""
            }
        }

        private val PARTICIPANT_STATS_COLUMNS = PARTICIPANT_STATS.columns.joinToString { it.name }
        private val PARTICIPANT_STATS_PARAMS = PARTICIPANT_STATS.columns.joinToString { "?" }

        //On insertion conflict we should only update non-null values.
        private val PARTICIPANT_STATS_UPDATE_PARAMS = (PARTICIPANT_STATS.columns - PARTICIPANT_STATS.primaryKey)
            .joinToString { "${it.name} = COALESCE(EXCLUDED.${it.name},${PARTICIPANT_STATS.name}.${it.name})" }

        /**
         * PreparedStatement bind order
         * 1) studyId
         */
        private val GET_STUDY_PARTICIPANT_STATS = """
            SELECT $PARTICIPANT_STATS_COLUMNS
            FROM ${PARTICIPANT_STATS.name}
            WHERE ${STUDY_ID.name} = ?
        """.trimIndent()

        /**
         * PreparedStatement bind order
         * 1) studyId
         * 2) participant id
         */
        private val GET_PARTICIPANT_STATS = """
            SELECT $PARTICIPANT_STATS_COLUMNS
            FROM ${PARTICIPANT_STATS.name}
            WHERE ${STUDY_ID.name} = ? 
            AND ${PARTICIPANT_ID.name} = ?
        """.trimIndent()

        val INSERT_OR_UPDATE_PARTICIPANT_STATS = """
            INSERT INTO ${PARTICIPANT_STATS.name} ($PARTICIPANT_STATS_COLUMNS)
            VALUES ($PARTICIPANT_STATS_PARAMS)
            ON CONFLICT (${STUDY_ID.name}, ${PARTICIPANT_ID.name}) 
            DO UPDATE SET $PARTICIPANT_STATS_UPDATE_PARAMS
        """.trimIndent()

        /**
         * PreparedStatement bind order
         * 1) participationStatus
         * 2) studyId
         * 3) particpantId
         */
        val SET_PARTICIPATION_STATUS_SQL = """
            UPDATE ${STUDY_PARTICIPANTS.name}
            SET ${PARTICIPATION_STATUS.name} = ? 
            WHERE ${STUDY_ID.name} = ? AND ${PARTICIPANT_ID.name} = ?
        """.trimIndent()
    }

    override fun createStudy(study: Study): UUID {
        val (flavor, hds) = storageResolver.getDefaultPlatformStorage()
        check(flavor == PostgresFlavor.VANILLA) { "Only vanilla postgres supported for studies." }
        study.id = idGenerationService.getNextId()
        val aclKey = AclKey(study.id)
        hds.connection.use { connection ->
            AuditedTransactionBuilder<Unit>(connection, auditingManager)
                .transaction { createStudy(it, study) }
                .audit {
                    listOf(
                        AuditableEvent(
                            aclKey,
                            eventType = AuditEventType.CREATE_STUDY,
                            description = "",
                            study = study.id,
                            organization = IdConstants.UNINITIALIZED.id,
                            data = mapOf()
                        )
                    ) + study.organizationIds.map { organizationId ->
                        AuditableEvent(
                            aclKey,
                            eventType = AuditEventType.ASSOCIATE_STUDY,
                            description = "",
                            study = study.id,
                            organization = organizationId,
                            data = mapOf()
                        )
                    }
                }
                .buildAndRun()
        }

        """
           CREATE TEMPORARY TABLE t2 AS SELECT study_id,participant_id,app_package_name,interaction_type,event_type,event_timestamp,timezone,username,application_label, min(uploaded_at as uploaded_at FROM chronicle_usage_events
        WHERE event_timestamp >= '-infinity' AND event_timestamp <= 'infinity' 
        GROUP BY study_id,participant_id,app_package_name,interaction_type,event_type,event_timestamp,timezone,username,application_label
        HAVING count(uploaded_at) > 1
        """.trimIndent()
        authorizationService.ensureAceIsLoaded(aclKey, Principals.getCurrentUser())
        return study.id
    }

    override fun expireStudies(studyIds: Set<UUID>) {
        studyIds.forEach { studyId ->
            val aclKey = AclKey(studyId)
            //Ensure admin role still has access to the study by default.
            authorizationService.addPermission(aclKey, Principals.getAdminRole(), EnumSet.allOf(Permission::class.java))

            authorizationService
                .getAllSecurableObjectPermissions(aclKey)
                .aces.filterNot { it.principal == Principals.getAdminRole() }
                .forEach { ace ->
                    authorizationService.removePermission(aclKey, ace.principal, EnumSet.copyOf(ace.permissions))
                }
        }
    }

    override fun createStudy(connection: Connection, study: Study) {
        insertStudy(connection, study)
        insertOrgStudy(connection, study)
        studyLimitsMgr.initializeStudyLimits(connection, study.id)
        surveysManager.initializeFilterdApps(connection, study.id)
        val aclKey = AclKey(study.id)
        authorizationService.createUnnamedSecurableObject(
            connection = connection,
            aclKey = aclKey,
            principal = Principals.getCurrentUser(),
            objectType = SecurableObjectType.Study
        )

        //Give admins access to the study by default.
        authorizationService.addPermission(aclKey, Principals.getAdminRole(), EnumSet.allOf(Permission::class.java))
    }

    private fun insertStudy(connection: Connection, study: Study): Int {
        return connection.prepareStatement(INSERT_STUDY_SQL).use { ps ->
            ps.setObject(1, study.id)
            ps.setObject(2, study.title)
            ps.setObject(3, study.description)
            ps.setObject(4, study.lat)
            ps.setObject(5, study.lon)
            ps.setObject(6, study.group)
            ps.setObject(7, study.version)
            ps.setObject(8, study.contact)
            ps.setObject(9, study.notificationsEnabled)
            ps.setObject(10, study.storage)
            ps.setString(11, mapper.writeValueAsString(study.settings))
            ps.setString(12, mapper.writeValueAsString(study.modules))
            ps.setString(13, study.phoneNumber)
            return ps.executeUpdate()
        }
    }

    private fun insertOrgStudy(connection: Connection, study: Study): Int {
        return connection.prepareStatement(INSERT_ORG_STUDIES_SQL).use { ps ->
            study.organizationIds.forEach { organizationId ->
                var params = 1
                ps.setObject(params++, organizationId)
                ps.setObject(params++, study.id)
                ps.setObject(params++, Principals.getCurrentUser().id)
                ps.setString(
                    params,
                    mapper.writeValueAsString(mapOf<String, Any>())
                ) //Per org settings for studies aren't really defined yet.
                ps.addBatch()
            }
            return ps.executeBatch().sum()
        }
    }

    override fun getStudy(studyId: UUID): Study {
        return getStudies(listOf(studyId)).first()
    }

    override fun getStudies(studyIds: Collection<UUID>): Iterable<Study> {
        return studies.getAll(studyIds.toSet()).values
//        return BasePostgresIterable(
//            PreparedStatementHolderSupplier(storageResolver.getPlatformStorage(), GET_STUDIES_SQL, 256) { ps ->
//                val pgStudyIds = PostgresArrays.createUuidArray(ps.connection, studyIds)
//                ps.setArray(1, pgStudyIds)
//                ps.executeQuery()
//            }
//        ) { ResultSetAdapters.study(it) }
    }

    override fun getOrgStudies(organizationId: UUID): List<Study> {
        //TODO: Retrieve studies from the cache.
        return BasePostgresIterable(
            PreparedStatementHolderSupplier(storageResolver.getPlatformStorage(), GET_ORG_STUDIES_SQL, 256) { ps ->
                ps.setObject(1, Principals.getCurrentUser().id)
                ps.setObject(2, organizationId)
                ps.executeQuery()
            }
        ) { ResultSetAdapters.study(it) }.toList()
    }

    override fun updateStudy(connection: Connection, studyId: UUID, study: StudyUpdate) {
        connection.prepareStatement(UPDATE_STUDY_SQL).use { ps ->
            ps.setString(1, study.title)
            ps.setString(2, study.description)
            ps.setObject(3, OffsetDateTime.now()) //Set last updated field.
            ps.setObject(4, study.lat)
            ps.setObject(5, study.lon)
            ps.setString(6, study.group)
            ps.setString(7, study.version)
            ps.setString(8, study.contact)
            ps.setObject(9, study.notificationsEnabled)
            ps.setString(10, study.storage)
            ps.setObject(11, if (study.settings == null) null else mapper.writeValueAsString(study.settings))
            ps.setString(12, if (study.modules == null) null else mapper.writeValueAsString(study.modules))
            ps.setObject(13, studyId)
            ps.executeUpdate()
        }
    }

    override fun getStudyPhoneNumber(studyId: UUID): String? {
        val realStudyId = getStudyId(studyId)
        checkNotNull(realStudyId) { "invalid study id" }
        return checkNotNull(studies.executeOnKey(realStudyId, STUDY_PHONE_NUMBER_GETTER))
    }

    override fun updateParticipationStatus(
        studyId: UUID,
        participantId: String,
        participationStatus: ParticipationStatus,
    ) {
        logger.info("Updating participation status: ${ChronicleServerUtil.STUDY_PARTICIPANT}", studyId, participantId)
        storageResolver.getPlatformStorage().connection.use { connection ->
            AuditedTransactionBuilder<Unit>(connection, auditingManager)
                .transaction { conn ->
                    conn.prepareStatement(SET_PARTICIPATION_STATUS_SQL).use { ps ->
                        ps.setString(1, participationStatus.name)
                        ps.setObject(2, studyId)
                        ps.setString(3, participantId)
                        ps.executeUpdate()
                    }
                }.audit {
                    listOf(
                        AuditableEvent(
                            aclKey = AclKey(studyId),
                            eventType = AuditEventType.UPDATE_PARTICIPATION_STATUS,
                            description = "Set participation status of participant $participantId in study $studyId to $participationStatus"
                        )
                    )
                }.buildAndRun()
        }
    }

    override fun registerParticipant(studyId: UUID, participant: Participant): UUID {
        val candidateId = storageResolver.getPlatformStorage().connection.use { conn ->
            AuditedTransactionBuilder<UUID>(conn, auditingManager)
                .transaction { connection -> registerParticipant(connection, studyId, participant) }
                .audit { candidateId ->
                    listOf(
                        AuditableEvent(
                            AclKey(candidateId),
                            eventType = AuditEventType.REGISTER_CANDIDATE,
                            description = "Registering participant with $candidateId for study $studyId."
                        )
                    )
                }
                .buildAndRun()
        }
        authorizationService.ensureAceIsLoaded(AclKey(candidateId), Principals.getCurrentUser())
        return candidateId
    }

    override fun registerParticipant(connection: Connection, studyId: UUID, participant: Participant): UUID {
        studyLimitsMgr.reserveEnrollmentCapacity(connection, studyId)
        val candidateId = candidateService.registerCandidate(connection, participant.candidate)
        enrollmentService.registerParticipant(
            connection,
            studyId,
            participant.participantId,
            candidateId,
            participant.participationStatus
        )
        val deliveryTypes = EnumSet.noneOf(DeliveryType::class.java)

        if (StringUtils.isNotBlank(participant.candidate.phoneNumber)) {
            deliveryTypes.add(DeliveryType.SMS)

        }
        if (StringUtils.isNotBlank(participant.candidate.email)) {
            deliveryTypes.add(DeliveryType.EMAIL)
        }

        try {
            val studySettings =
                getStudy(studyId).settings.getValue(StudySettingType.Notifications) as StudyNotificationSettings

            if (studySettings.notifyOnEnrollment) {
                notificationService.sendNotifications(
                    connection,
                    studyId,
                    listOf(
                        ParticipantNotification(
                            participant.participantId,
                            NotificationType.ENROLLMENT,
                            deliveryTypes,
                            message = studySettings.getEnrollmentMessage()
                        )
                    )
                )
            }
        } catch (ex: Exception) {
            //If something goes wrong with sending out notifications keep it going.
            logger.error("Unable to send out notifications.", ex)
        }
        return candidateId
    }

    override fun isNotificationsEnabled(studyId: UUID): Boolean {
        logger.info("Checking notifications enabled on studyId = {}", studyId)

        return storageResolver.getPlatformStorage().connection.use { connection ->
            connection.prepareStatement(GET_NOTIFICATION_STATUS_SQL).use { ps ->
                ps.setObject(1, studyId)
                ps.executeQuery().use { rs ->
                    if (rs.next()) rs.getBoolean(NOTIFICATIONS_ENABLED.name)
                    else false
                }
            }
        }
    }

    override fun getOrganizationIdForLegacyStudy(studyId: UUID): UUID {
        return storageResolver.getPlatformStorage().connection.use { connection ->
            connection.prepareStatement(GET_ORGANIZATION_ID).use { ps ->
                ps.setObject(1, studyId)
                ps.executeQuery().use { rs ->
                    if (rs.next()) rs.getObject(ORGANIZATION_ID.name, UUID::class.java)
                    else IdConstants.SYSTEM_ORGANIZATION.id
                }
            }

        }
    }

    override fun refreshStudyCache(studyIds: Set<UUID>) {
        studies.loadAll(studyIds, true)
    }

    override fun deleteStudies(connection: Connection, studyIds: Collection<UUID>): Int {
        return connection.prepareStatement(DELETE_STUDIES_SQL).use { ps ->
            val pgStudyIds = PostgresArrays.createUuidArray(ps.connection, studyIds)
            ps.setArray(1, pgStudyIds)
            return ps.executeUpdate()
        }
    }

    override fun removeStudiesFromOrganizations(connection: Connection, studyIds: Collection<UUID>): Int {
        return connection.prepareStatement(REMOVE_STUDIES_FROM_ORGS_SQL).use { ps ->
            val pgStudyIds = PostgresArrays.createUuidArray(ps.connection, studyIds)
            ps.setArray(1, pgStudyIds)
            return ps.executeUpdate()
        }
    }

    override fun removeAllParticipantsFromStudies(connection: Connection, studyIds: Collection<UUID>): Int {
        return connection.prepareStatement(REMOVE_ALL_PARTICIPANTS_FROM_STUDIES_SQL).use { ps ->
            val pgStudyIds = PostgresArrays.createUuidArray(ps.connection, studyIds)
            ps.setArray(1, pgStudyIds)
            return ps.executeUpdate()
        }
    }

    override fun removeParticipantsFromStudy(
        connection: Connection,
        studyId: UUID,
        participantIds: Collection<String>,
    ): Int {
        return connection.prepareStatement(REMOVE_PARTICIPANTS_FROM_STUDY_SQL).use { ps ->
            ps.setObject(1, studyId)
            val pgParticipantIds = PostgresArrays.createTextArray(ps.connection, participantIds)
            ps.setObject(2, pgParticipantIds)
            return ps.executeUpdate()
        }
    }

    override fun getStudySettings(studyId: UUID): Map<StudySettingType, StudySetting> {
        return storageResolver.getPlatformStorage().connection.use { connection ->
            connection.prepareStatement(GET_STUDY_SETTINGS_SQL).use { ps ->
                ps.setObject(1, studyId)
                ps.executeQuery().use { rs ->
                    if (rs.next()) mapper.readValue(rs.getString(SETTINGS.name))
                    else mapOf()
                }
            }
        }
    }

    override fun getStudySensors(studyId: UUID): Set<SensorType> {
        val settings = getStudySettings(studyId)
        return settings[StudySettingType.Sensor] as SensorSetting? ?: SensorSetting.NO_SENSORS
    }

    override fun getStudyParticipantStats(studyId: UUID): Map<String, ParticipantStats> {
        val hds = storageResolver.getPlatformStorage()
        return BasePostgresIterable(
            PreparedStatementHolderSupplier(hds, GET_STUDY_PARTICIPANT_STATS) { ps ->
                ps.setObject(1, studyId)
            }
        ) { ResultSetAdapters.participantStats(it) }
            .associateBy { it.participantId }
    }

    override fun getParticipantStats(studyId: UUID, participantId: String): ParticipantStats? {
        val hds = storageResolver.getPlatformStorage()
        return try {
            BasePostgresIterable(
                PreparedStatementHolderSupplier(
                    hds, GET_PARTICIPANT_STATS
                ) { ps ->
                    ps.setObject(1, studyId)
                    ps.setString(2, participantId)
                }
            ) { ResultSetAdapters.participantStats(it) }.first()
        } catch (ex: Exception) {
            return null
        }
    }

    override fun insertOrUpdateParticipantStats(stats: ParticipantStats) {
        storageResolver.getPlatformStorage().connection.use { connection ->
            connection.prepareStatement(INSERT_OR_UPDATE_PARTICIPANT_STATS).use { ps ->
                val androidDatesArr =
                    connection.createArrayOf(PostgresDatatype.DATE.sql(), stats.androidUniqueDates.toTypedArray())
                val iosDatesArr =
                    connection.createArrayOf(PostgresDatatype.DATE.sql(), stats.iosUniqueDates.toTypedArray())
                val tudDatesArr =
                    connection.createArrayOf(PostgresDatatype.DATE.sql(), stats.tudUniqueDates.toTypedArray())

                var index = 0

                ps.setObject(++index, stats.studyId)
                ps.setString(++index, stats.participantId)

                ps.setObject(++index, stats.androidLastPing)
                ps.setObject(++index, stats.androidFirstDate)
                ps.setObject(++index, stats.androidLastDate)
                ps.setArray(++index, androidDatesArr)
                ps.setObject(++index, stats.iosLastPing)
                ps.setObject(++index, stats.iosFirstDate)
                ps.setObject(++index, stats.iosLastDate)
                ps.setArray(++index, iosDatesArr)

                ps.setObject(++index, stats.tudFirstDate)
                ps.setObject(++index, stats.tudLastDate)
                ps.setObject(++index, tudDatesArr)

                ps.executeUpdate()
            }
        }

    }

    override fun getStudyParticipants(studyId: UUID): Iterable<Participant> {
        return selectStudyParticipants(studyId)
    }

    override fun countStudyParticipants(studyId: UUID): Long {
        return studyLimitsMgr.countStudyParticipants(studyId)
    }

    override fun countStudyParticipants(connection: Connection, studyIds: Set<UUID>): Map<UUID, Long> {
        return studyLimitsMgr.countStudyParticipants(connection, studyIds)
    }

    override fun countStudyParticipants(studyIds: Set<UUID>): Map<UUID, Long> {
        return studyLimitsMgr.countStudyParticipants(studyIds)
    }

    override fun updateLastDevicePing(studyId: UUID, participantId: String, sourceDevice: SourceDevice) {
        val participantStats = when (sourceDevice) {
            is AndroidDevice -> ParticipantStats(
                studyId = studyId,
                participantId = participantId,
                androidLastPing = OffsetDateTime.now()
            )
            is IOSDevice -> ParticipantStats(
                studyId = studyId,
                participantId = participantId,
                androidLastPing = OffsetDateTime.now()
            )
            else -> throw UnsupportedOperationException("${sourceDevice.javaClass.name} is not a supported datasource.")
        }

        insertOrUpdateParticipantStats(participantStats)
    }

    override fun updateLastDevicePing(studyId: UUID, participantId: String) {
        //If device hasn't enrolled yet, we should just return without updating last ping.
        val participantStats = getParticipantStats(studyId, participantId) ?: return

        if (participantStats.androidLastPing != null) {
            insertOrUpdateParticipantStats(
                ParticipantStats(
                    studyId,
                    participantId,
                    androidLastPing = OffsetDateTime.now()
                )
            )
        }

        if (participantStats.iosLastPing != null) {
            insertOrUpdateParticipantStats(ParticipantStats(studyId, participantId, iosLastPing = OffsetDateTime.now()))
        }
    }

    private fun selectStudyParticipants(studyId: UUID): Iterable<Participant> {
        val (flavor, hds) = storageResolver.getDefaultPlatformStorage()
        ensureVanilla(flavor)
        return BasePostgresIterable(
            PreparedStatementHolderSupplier(hds, SELECT_STUDY_PARTICIPANTS_SQL) { ps ->
                ps.setObject(1, studyId)
            }
        ) { ResultSetAdapters.participant(it) }
    }

    override fun getStudyId(maybeLegacyMaybeRealStudyId: UUID): UUID? {
        return storageResolver.getPlatformStorage().connection.use { connection ->
            var maybeStudyId = connection.prepareStatement(selectStudyIdSql(LEGACY_STUDY_IDS.name)).use { ps ->
                ps.setObject(1, maybeLegacyMaybeRealStudyId)
                ps.executeQuery().use { rs ->
                    if (rs.next()) ResultSetAdapters.studyId(rs) else null
                }
            }
            if (maybeStudyId == null) {
                maybeStudyId = connection.prepareStatement(selectStudyIdSql(STUDIES.name)).use { ps ->
                    ps.setObject(1, maybeLegacyMaybeRealStudyId)
                    ps.executeQuery().use { rs ->
                        if (rs.next()) ResultSetAdapters.studyId(rs) else null
                    }
                }
            }
            maybeStudyId
        }
    }

    override fun isValidStudy(studyId: UUID): Boolean {
        return getStudyId(studyId) != null
    }
}
