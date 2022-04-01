package com.openlattice.chronicle.services.studies

import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.postgres.PostgresArrays
import com.hazelcast.core.HazelcastInstance
import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.STUDY_LIMITS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PARTICIPANT_COUNT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PARTICIPANT_LIMIT
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
class StudyLimitsServce(
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
            INSERT INTO ${STUDY_LIMITS.name} VALUES(?,?,?,?,?) 
        """.trimIndent()
        private val RESERVE_ENROLLMENT = """
            UPDATE ${STUDY_LIMITS.name} SET ${PARTICIPANT_COUNT.name} = ${PARTICIPANT_COUNT.name} + ?
                WHERE ${STUDY_ID.name} = ? AND ${PARTICIPANT_COUNT.name} <= ?
         """.trimIndent()
        private val SET_PARTICIPANT_LIMIT = """
            UPDATE ${STUDY_LIMITS.name} SET ${PARTICIPANT_LIMIT.name} = ?WHERE ${STUDY_ID.name} = ?
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

    override fun reserveEnrollmentCapacity(connection: Connection, studyId: UUID, capacity: Int) {
        /**
         * Need to somehow keep track of
         */
        connection.prepareStatement(RESERVE_ENROLLMENT).use { ps ->
            ps.setObject(1, studyId)
            ps.setInt(2, capacity)
            ps.setInt(3, studyLimits.getValue(studyId).participantLimit - capacity)
            check(ps.executeUpdate() > 1) { "Insufficient capacity to enroll participants." }
        }
    }

    override fun getAvailableEnrollmentCapactity(studyId: UUID): Int {
        return studyLimits.getValue(studyId).participantLimit
    }

    override fun setEnrollmentCapacity(studyId: UUID, capacity: Int) {
        storageResolver.getPlatformStorage().connection.use { connection ->
            connection.prepareStatement(SET_PARTICIPANT_LIMIT).use { ps ->
                ps.setInt(1, capacity)
                ps.setObject(2, STUDY_ID)

            }
        }
    }

    override fun setStudyDuration(studyId: UUID, studyDuration: StudyDuration) {
        TODO("Not yet implemented")
    }

    override fun getStudyDuration(studyId: UUID): StudyDuration {
        TODO("Not yet implemented")
    }

    override fun setDataRetentionPeriod(studyId: UUID, dataRetentionPeriod: StudyDuration) {
        TODO("Not yet implemented")
    }

    override fun getDataRetentionPeriod(studyId: UUID): StudyDuration {
        TODO("Not yet implemented")
    }

    override fun makeArchivable(studyId: UUID) {
        TODO("Not yet implemented")
    }

    override fun isArchivable(studyId: UUID): Boolean {
        TODO("Not yet implemented")
    }

    override fun getStudyFeatures(studyId: UUID): Set<StudyFeature> {
        TODO("Not yet implemented")
    }

    override fun setStudyFeatureS(studyId: UUID, studyFeatures: Set<StudyFeature>) {
        TODO("Not yet implemented")
    }

    override fun setStudyLimits(studyId: UUID, studyLimits: StudyLimits) {
        TODO("Not yet implemented")
    }

    override fun getStudyLimits(studyId: UUID): StudyLimits {
        TODO("Not yet implemented")
    }

}