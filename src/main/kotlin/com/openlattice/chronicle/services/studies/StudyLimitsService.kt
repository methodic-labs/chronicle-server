package com.openlattice.chronicle.services.studies

import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.hazelcast.core.HazelcastInstance
import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.storage.ChroniclePostgresTables
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.STUDIES
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.STUDY_LIMITS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.CREATED_AT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.DATA_RETENTION
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ENDED_AT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.FEATURES
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PARTICIPANT_LIMIT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_DURATION
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.study.StudyDuration
import com.openlattice.chronicle.study.StudyFeature
import com.openlattice.chronicle.study.StudyLimits
import java.sql.Connection
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class StudyLimitsService(
    private val storageResolver: StorageResolver,
    hazelcast: HazelcastInstance
) : StudyLimitsManager {
    companion object {
        private val mapper = ObjectMappers.newJsonMapper()

        /**
         * 1. STUDY_ID
         * 2. PARTICIPANT_LIMIT
         * 3. STUDY_DURATION
         * 4. DATA_RETENTION
         * 5. FEATURES
         */
        private val INSERT_STUDY_LIMITS = """
            INSERT INTO ${STUDY_LIMITS.name} VALUES(?,?,?::jsonb,?,?) 
        """.trimIndent()
        private val LOCK_STUDY = """
            SELECT 1 FROM ${STUDY_LIMITS.name} WHERE ${STUDY_ID.name} = ? FOR UPDATE
         """.trimIndent()

        private val UPDATE_PARTICIPANT_LIMIT = """
            UPDATE ${STUDY_LIMITS.name} SET ${PARTICIPANT_LIMIT.name} = ? WHERE ${STUDY_ID.name} = ?
         """.trimIndent()

        private val UPDATE_STUDY_DURATION = """
            UPDATE ${STUDY_LIMITS.name} SET ${STUDY_DURATION.name} = ? WHERE ${STUDY_ID.name} = ?
         """.trimIndent()

        private val UPDATE_RETENTION_PERIOD = """
            UPDATE ${STUDY_LIMITS.name} SET ${DATA_RETENTION.name} = ? WHERE ${STUDY_ID.name} = ?
         """.trimIndent()

        private val UPDATE_STUDY_FEATURES = """
            UPDATE ${STUDY_LIMITS.name} SET ${FEATURES.name} = ? WHERE ${STUDY_ID.name} = ?
         """.trimIndent()

        private val STUDIES_EXCEEDING_DURATION_LIMIT = """
            SELECT * FROM ${STUDIES.name} INNER JOIN ${STUDY_LIMITS.name} USING (${STUDY_ID.name}) 
            WHERE (now() - ${CREATED_AT.name}) > INTERVAL (${STUDY_DURATION.name}->>'years' || ' years ' || 
                {$STUDY_DURATION.name}->>'months' || ' months ' || 
                {$STUDY_DURATION.name}->>'days' || ' days ')
        """.trimIndent()
        private val STUDIES_EXCEEDING_RETENTION_LIMIT = """
            SELECT * FROM ${STUDIES.name} INNER JOIN ${STUDY_LIMITS.name} USING (${STUDY_ID.name}) 
            WHERE (now() - ${ENDED_AT.name}) > INTERVAL (${DATA_RETENTION.name}->>'years' || ' years ' || 
                {$DATA_RETENTION.name}->>'months' || ' months ' || 
                {$DATA_RETENTION.name}->>'days' || ' days ')
        """.trimIndent()
        private val COUNT_STUDY_PARTICIPANTS_SQL = """
            SELECT ${STUDY_ID.name}, count(*) FROM ${ChroniclePostgresTables.STUDY_PARTICIPANTS.name} WHERE ${STUDY_ID.name} = ANY(?)
        """.trimIndent()
    }

    private val studyLimits = HazelcastMap.STUDY_LIMITS.getMap(hazelcast)

    override fun initializeStudyLimits(connection: Connection, studyId: UUID, studyLimits: StudyLimits) {
        connection.prepareStatement(INSERT_STUDY_LIMITS).use { ps ->
            ps.setObject(1, studyId)
            ps.setInt(2, studyLimits.participantLimit)
            ps.setString(3, mapper.writeValueAsString(studyLimits.studyDuration))
            ps.setString(4, mapper.writeValueAsString(studyLimits.dataRetentionDuration))
            ps.setArray(5, PostgresArrays.createTextArray(ps.connection, studyLimits.features.map { it.name }))
            ps.execute()
        }
    }

    override fun lockStudyForEnrollments(connection: Connection, studyId: UUID) {
        connection.prepareStatement(LOCK_STUDY).use { ps -> ps.setObject(1, studyId) }
    }

    override fun getEnrollmentCapacity(studyId: UUID): Int {
        return studyLimits.getValue(studyId).participantLimit
    }

    override fun setEnrollmentCapacity(studyId: UUID, capacity: Int) {
        storageResolver.getPlatformStorage().connection.use { connection ->
            connection.prepareStatement(UPDATE_PARTICIPANT_LIMIT).use { ps ->
                ps.setInt(1, capacity)
                ps.setObject(2, STUDY_ID)

            }
        }
        studyLimits.loadAll(setOf(studyId), true)
    }

    override fun setStudyDuration(studyId: UUID, studyDuration: StudyDuration) {
        storageResolver.getPlatformStorage().connection.use { connection ->
            connection.prepareStatement(UPDATE_STUDY_DURATION).use { ps ->
                ps.setString(1, mapper.writeValueAsString(studyDuration))
                ps.setObject(2, STUDY_ID)

            }
        }
        studyLimits.loadAll(setOf(studyId), true)
    }

    override fun getStudyDuration(studyId: UUID): StudyDuration {
        return studyLimits.getValue(studyId).studyDuration
    }

    override fun setDataRetentionPeriod(studyId: UUID, dataRetentionPeriod: StudyDuration) {
        storageResolver.getPlatformStorage().connection.use { connection ->
            connection.prepareStatement(UPDATE_RETENTION_PERIOD).use { ps ->
                ps.setString(1, mapper.writeValueAsString(dataRetentionPeriod))
                ps.setObject(2, STUDY_ID)

            }
        }
        studyLimits.loadAll(setOf(studyId), true)
    }

    override fun getDataRetentionPeriod(studyId: UUID): StudyDuration {
        return studyLimits.getValue(studyId).dataRetentionDuration
    }

    override fun getStudyFeatures(studyId: UUID): Set<StudyFeature> {
        return studyLimits.getValue(studyId).features
    }

    override fun setStudyFeatures(studyId: UUID, studyFeatures: Set<StudyFeature>) {
        storageResolver.getPlatformStorage().connection.use { connection ->
            connection.prepareStatement(UPDATE_STUDY_FEATURES).use { ps ->
                ps.setArray(1, PostgresArrays.createTextArray(connection, studyFeatures.map { it.name }))
                ps.setObject(2, STUDY_ID)

            }
        }
        studyLimits.loadAll(setOf(studyId), true)
    }

    override fun setStudyLimits(studyId: UUID, studyLimits: StudyLimits) {
        this.studyLimits[studyId] = studyLimits
    }

    override fun getStudyLimits(studyId: UUID): StudyLimits {
        return studyLimits.getValue(studyId)
    }

    override fun getStudiesExceedingDurationLimit(): Set<UUID> {
        return BasePostgresIterable(
            PreparedStatementHolderSupplier(
                storageResolver.getPlatformStorage(),
                STUDIES_EXCEEDING_DURATION_LIMIT
            ) { }
        ) { ResultSetAdapters.studyId(it) }.toSet()
    }

    override fun getStudiesExcceedingDataRetentionPeriod(): Set<UUID> {
        return BasePostgresIterable(
            PreparedStatementHolderSupplier(
                storageResolver.getPlatformStorage(),
                STUDIES_EXCEEDING_RETENTION_LIMIT
            ) { }
        ) { ResultSetAdapters.studyId(it) }.toSet()
    }

    override fun countStudyParticipants(studyId: UUID): Long {
        val hds = storageResolver.getPlatformStorage()
        return hds.connection.use { connection ->
            countStudyParticipants(connection, setOf(studyId)).values.first()
        }
    }

    override fun countStudyParticipants(studyIds: Set<UUID>): Map<UUID, Long> {
        val hds = storageResolver.getPlatformStorage()
        return hds.connection.use { connection ->
            countStudyParticipants(connection, studyIds)
        }
    }

    override fun countStudyParticipants(connection: Connection, studyIds: Set<UUID>): Map<UUID, Long> {
        val studyCounts = mutableMapOf<UUID, Long>()
        connection.prepareStatement(COUNT_STUDY_PARTICIPANTS_SQL).use { ps ->
            ps.setArray(1, PostgresArrays.createUuidArray(connection, studyIds))
            ps.executeQuery().use { rs ->
                while (rs.next()) {
                    studyCounts[ResultSetAdapters.studyId(rs)] = ResultSetAdapters.count(rs)
                }
            }
        }
        return studyCounts
    }
}