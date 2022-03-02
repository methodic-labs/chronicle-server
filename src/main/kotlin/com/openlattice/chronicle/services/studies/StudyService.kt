package com.openlattice.chronicle.services.studies

import com.fasterxml.jackson.module.kotlin.readValue
import com.geekbeast.configuration.postgres.PostgresFlavor
import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.PostgresDatatype
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.hazelcast.core.HazelcastInstance
import com.openlattice.chronicle.auditing.AuditEventType
import com.openlattice.chronicle.auditing.AuditableEvent
import com.openlattice.chronicle.auditing.AuditedOperationBuilder
import com.openlattice.chronicle.auditing.AuditingComponent
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.Permission.READ
import com.openlattice.chronicle.authorization.SecurableObjectType
import com.openlattice.chronicle.authorization.principals.Principals
import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.ids.IdConstants
import com.openlattice.chronicle.participants.Participant
import com.openlattice.chronicle.participants.ParticipantStats
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.sensorkit.SensorType
import com.openlattice.chronicle.services.candidates.CandidateManager
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
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
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ENDED_AT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.LAT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.LEGACY_STUDY_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.LON
import com.openlattice.chronicle.storage.PostgresColumns.Companion.NOTIFICATIONS_ENABLED
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ORGANIZATION_IDS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PRINCIPAL_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SETTINGS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STARTED_AT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STORAGE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_GROUP
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_VERSION
import com.openlattice.chronicle.storage.PostgresColumns.Companion.TITLE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.UPDATED_AT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.USER_ID
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.study.Study
import com.openlattice.chronicle.study.StudyUpdate
import com.openlattice.chronicle.util.ensureVanilla
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.sql.Connection
import java.time.OffsetDateTime
import java.util.UUID

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
@Service
class StudyService(
    private val storageResolver: StorageResolver,
    private val authorizationService: AuthorizationManager,
    private val candidateService: CandidateManager,
    private val enrollmentService: EnrollmentManager,
    private val idGenerationService: HazelcastIdGenerationService,
    override val auditingManager: AuditingManager,
    hazelcast: HazelcastInstance,
) : StudyManager, AuditingComponent {
    private val studies = HazelcastMap.STUDIES.getMap(hazelcast)

    companion object {
        private val logger = LoggerFactory.getLogger(StudyService::class.java)
        private val mapper = ObjectMappers.newJsonMapper()
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
            SETTINGS
        )
        private val STUDY_COLUMNS = STUDY_COLUMNS_LIST.joinToString(",") { it.name }
        private val STUDY_COLUMNS_BIND = STUDY_COLUMNS_LIST.joinToString(",") {
            if (it.datatype == PostgresDatatype.JSONB) "?::jsonb" else "?"
        }

        private val UPDATE_STUDY_COLUMNS_LIST = listOf(
            TITLE,
            DESCRIPTION,
            UPDATED_AT,
            STARTED_AT,
            ENDED_AT,
            LAT,
            LON,
            STUDY_GROUP,
            STUDY_VERSION,
            CONTACT,
            NOTIFICATIONS_ENABLED,
            STORAGE,
            SETTINGS
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

        /**
         * 1. title,
         * 2. description,
         * 3. updated_at,
         * 4. started_at,
         * 5. ended_at,
         * 6. lat,
         * 7. lon,
         * 8. study_group,
         * 9. study_version,
         * 10. contact,
         * 11. notifications enabled
         * 12. storage
         * 13. settings
         * 14. study_id
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

        private fun selectStudyIdSql(table: String) :String {
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
        private val PARTICIPANT_STATS_UPDATE_PARAMS = PARTICIPANT_STATS.columns.joinToString { "${it.name} = ?" }
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
            INSERT INTO ${PARTICIPANT_STATS.name}
            VALUES ($PARTICIPANT_STATS_PARAMS)
            ON CONFLICT (${STUDY_ID.name}, ${PARTICIPANT_ID.name}) 
            DO UPDATE SET $PARTICIPANT_STATS_UPDATE_PARAMS
        """.trimIndent()
    }

    override fun createStudy( study: Study ) : UUID {
        val (flavor, hds) = storageResolver.getDefaultPlatformStorage()
        check(flavor == PostgresFlavor.VANILLA) { "Only vanilla postgres supported for studies." }
        study.id = idGenerationService.getNextId()

        hds.connection.use { connection ->
            AuditedOperationBuilder<Unit>(connection, auditingManager)
                .operation { connection -> createStudy(connection, study) }
                .audit {
                    listOf(
                        AuditableEvent(
                            AclKey(study.id),
                            eventType = AuditEventType.CREATE_STUDY,
                            description = "",
                            study = study.id,
                            organization = IdConstants.UNINITIALIZED.id,
                            data = mapOf()
                        )
                    ) + study.organizationIds.map { organizationId ->
                        AuditableEvent(
                            AclKey(study.id),
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
        return study.id
    }

    override fun createStudy(connection: Connection, study: Study) {
        insertStudy(connection, study)
        insertOrgStudy(connection, study)
        authorizationService.createUnnamedSecurableObject(
            connection = connection,
            aclKey = AclKey(study.id),
            principal = Principals.getCurrentUser(),
            objectType = SecurableObjectType.Study
        )
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
        return BasePostgresIterable(
            PreparedStatementHolderSupplier(storageResolver.getPlatformStorage(), GET_STUDIES_SQL, 256) { ps ->
                val pgStudyIds = PostgresArrays.createUuidArray(ps.connection, studyIds)
                ps.setArray(1, pgStudyIds)
                ps.executeQuery()
            }
        ) { ResultSetAdapters.study(it) }
    }

    override fun getOrgStudies(organizationId: UUID): List<Study> {
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
            ps.setObject(3, OffsetDateTime.now())
            ps.setObject(4, study.startedAt)
            ps.setObject(5, study.endedAt)
            ps.setObject(6, study.lat)
            ps.setObject(7, study.lon)
            ps.setString(8, study.group)
            ps.setString(9, study.version)
            ps.setString(10, study.contact)
            ps.setObject(11, study.notificationsEnabled)
            ps.setString(12, study.storage)
            if (study.settings == null) {
                ps.setObject(13, study.settings)
            } else {
                ps.setString(13, mapper.writeValueAsString(study.settings))
            }
            ps.setObject(14, studyId)
            ps.executeUpdate()
        }
    }

    override fun registerParticipant( studyId: UUID, participant: Participant) : UUID {
        return storageResolver.getPlatformStorage().connection.use { conn ->
            AuditedOperationBuilder<UUID>(conn, auditingManager)
                .operation { connection -> registerParticipant(connection, studyId, participant) }
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
    }
    override fun registerParticipant(connection: Connection, studyId: UUID, participant: Participant): UUID {
        val candidateId = candidateService.registerCandidate(connection, participant.candidate)
        enrollmentService.registerParticipant(
            connection,
            studyId,
            participant.participantId,
            candidateId,
            participant.participationStatus
        )
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

    fun removeStudiesFromOrganizations(connection: Connection, studyIds: Collection<UUID>): Int {
        return connection.prepareStatement(REMOVE_STUDIES_FROM_ORGS_SQL).use { ps ->
            val pgStudyIds = PostgresArrays.createUuidArray(ps.connection, studyIds)
            ps.setArray(1, pgStudyIds)
            return ps.executeUpdate()
        }
    }

    fun removeAllParticipantsFromStudies(connection: Connection, studyIds: Collection<UUID>): Int {
        return connection.prepareStatement(REMOVE_ALL_PARTICIPANTS_FROM_STUDIES_SQL).use { ps ->
            val pgStudyIds = PostgresArrays.createUuidArray(ps.connection, studyIds)
            ps.setArray(1, pgStudyIds)
            return ps.executeUpdate()
        }
    }

    override fun getStudySettings(studyId: UUID): Map<String, Any>{
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
        val sensors = settings[Study.SENSORS]
        sensors?.let {
            return (it as List<String>).map { sensor -> SensorType.valueOf(sensor) }.toSet()
        }
        return setOf()
    }

    override fun getStudyParticipantStats(studyId: UUID): Map<String, ParticipantStats> {
        val hds = storageResolver.getPlatformStorage()
        return BasePostgresIterable(
            PreparedStatementHolderSupplier(hds, GET_STUDY_PARTICIPANT_STATS) { ps ->
                ps.setObject(1, studyId)
            }
        ) { ResultSetAdapters.participantStats(it)}
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
            ) { ResultSetAdapters.participantStats(it)}.first()
        } catch (ex: Exception) {
            return null
        }
    }

    override fun insertOrUpdateParticipantStats(stats: ParticipantStats) {
        storageResolver.getPlatformStorage().connection.use { connection ->
            connection.prepareStatement(INSERT_OR_UPDATE_PARTICIPANT_STATS).use { ps ->
                val androidDatesArr =  connection.createArrayOf(PostgresDatatype.DATE.sql(), stats.androidUniqueDates.toTypedArray())
                val iosDatesArr = connection.createArrayOf(PostgresDatatype.DATE.sql(), stats.iosUniqueDates.toTypedArray())
                val tudDatesArr = connection.createArrayOf(PostgresDatatype.DATE.sql(), stats.tudUniqueDates.toTypedArray())

                var index = 0

                for (i in 0..1) {
                    ps.setObject(++index, stats.studyId)
                    ps.setString(++index, stats.participantId)
                    ps.setObject(++index, stats.androidFirstDate)
                    ps.setObject(++index, stats.androidLastDate)
                    ps.setArray(++index, androidDatesArr)
                    ps.setObject(++index, stats.iosFirstDate)
                    ps.setObject(++index, stats.iosLastDate)
                    ps.setArray(++index, iosDatesArr)
                    ps.setObject(++index, stats.tudFirstDate)
                    ps.setObject(++index, stats.tudLastDate)
                    ps.setObject(++index, tudDatesArr)
                }
                ps.executeUpdate()
            }
        }
    }

    override fun getStudyParticipants(studyId: UUID): Iterable<Participant> {
        return selectStudyParticipants(studyId)
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
