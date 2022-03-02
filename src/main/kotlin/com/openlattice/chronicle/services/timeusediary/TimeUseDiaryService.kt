package com.openlattice.chronicle.services.timeusediary

import com.geekbeast.configuration.postgres.PostgresFlavor
import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.openlattice.chronicle.converters.PostgresDownloadWrapper
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.participants.ParticipantStats
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.services.studies.StudyService
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.TIME_USE_DIARY_SUBMISSIONS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SUBMISSION
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SUBMISSION_DATE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SUBMISSION_ID
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.timeusediary.TimeUseDiaryDownloadDataType
import com.openlattice.chronicle.timeusediary.TimeUseDiaryResponse
import com.openlattice.chronicle.util.ChronicleServerUtil
import org.postgresql.util.PGobject
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Types
import java.time.OffsetDateTime
import java.util.*

/**
 * @author Andrew Carter andrew@openlattice.com
 */
class TimeUseDiaryService(
    private val storageResolver: StorageResolver,
    private val idGenerationService: HazelcastIdGenerationService,
    private val studyService: StudyService
) : TimeUseDiaryManager {

    companion object {
        private val logger = LoggerFactory.getLogger(TimeUseDiaryService::class.java)
        private val mapper = ObjectMappers.getJsonMapper()
    }

    override fun submitTimeUseDiary(
        connection: Connection,
        organizationId: UUID,
        studyId: UUID,
        participantId: String,
        responses: List<TimeUseDiaryResponse>
    ): UUID {
        val timeUseDiaryId = idGenerationService.getNextId()
        val submissionDate = OffsetDateTime.now()
        executeInsertTimeUseDiarySql(
            connection,
            timeUseDiaryId,
            organizationId,
            studyId,
            participantId,
            responses,
            submissionDate
        )
        updateParticipantStats(studyId, participantId, submissionDate)
        logger.info("submitted time use diary responses ${ChronicleServerUtil.STUDY_PARTICIPANT}", studyId, participantId)
        return timeUseDiaryId
    }

    private fun updateParticipantStats(studyId: UUID, participantId: String, submissionDate: OffsetDateTime) {
        val currentStats = studyService.getParticipantStats(studyId, participantId)
        val uniqueDates: MutableSet<LocalDate> = (currentStats?.tudUniqueDates?.toMutableSet() ?: mutableSetOf())
        uniqueDates += submissionDate.toLocalDate()

        val participantStats = ParticipantStats(
            studyId = studyId,
            participantId = participantId,
            tudUniqueDates = uniqueDates,
            tudFirstDate = currentStats?.tudFirstDate,
            tudLastDate = submissionDate,
            androidFirstDate = currentStats?.androidFirstDate,
            androidLastDate = currentStats?.androidLastDate,
            androidUniqueDates = currentStats?.androidUniqueDates ?: setOf(),
            iosFirstDate = currentStats?.iosFirstDate,
            iosLastDate = currentStats?.iosLastDate,
            iosUniqueDates = currentStats?.iosUniqueDates ?: setOf()
        )
        studyService.insertOrUpdateParticipantStats(participantStats)
    }

    override fun getParticipantTUDSubmissionsByDate(
        organizationId: UUID,
        studyId: UUID,
        participantId: String,
        startDate: OffsetDateTime,
        endDate: OffsetDateTime,
    ): Map<OffsetDateTime, Set<UUID>> {
        val submissionsByDate = mutableMapOf<OffsetDateTime, MutableSet<UUID>>()
        try {
            val hds = storageResolver.getPlatformStorage(PostgresFlavor.VANILLA)
            val result = hds.connection.use { connection ->
                executeGetSubmissionByDateSql(
                    connection,
                    organizationId,
                    studyId,
                    participantId,
                    startDate,
                    endDate
                )
            }
            while (result.next()) {
                val date = ResultSetAdapters.submissionDate(result)
                val id = ResultSetAdapters.submissionId(result)
                if (submissionsByDate[date].isNullOrEmpty()) {
                    submissionsByDate[date] = mutableSetOf(id)
                } else {
                    submissionsByDate[date]!!.add(id)
                }
            }
        } catch (ex: SQLException) {
            logger.error("hds error: $ex")
        }
        return submissionsByDate
    }

    override fun getStudyTUDSubmissionsByDate(
        studyId: UUID,
        startDate: OffsetDateTime,
        endDate: OffsetDateTime,
    ): Map<OffsetDateTime, Set<UUID>> {
        val submissionsByDate = mutableMapOf<OffsetDateTime, MutableSet<UUID>>()
        try {
            val hds = storageResolver.getPlatformStorage(PostgresFlavor.VANILLA)
            val result = hds.connection.use { connection ->
                executeGetStudyTUDSubmissionsByDateSql(
                    connection,
                    studyId,
                    startDate,
                    endDate
                )
            }
            while (result.next()) {
                val date = ResultSetAdapters.submissionDate(result)
                val id = ResultSetAdapters.submissionId(result)
                if (submissionsByDate[date].isNullOrEmpty()) {
                    submissionsByDate[date] = mutableSetOf(id)
                } else {
                    submissionsByDate[date]!!.add(id)
                }
            }
        } catch (ex: SQLException) {
            logger.error("hds error: $ex")
        }
        return submissionsByDate
    }

    override fun downloadTimeUseDiaryData(
        organizationId: UUID,
        studyId: UUID,
        participantId: String,
        downloadType: TimeUseDiaryDownloadDataType,
        submissionIds: Set<UUID>
    ): PostgresDownloadWrapper {
        try {
            val hds = storageResolver.getPlatformStorage(PostgresFlavor.VANILLA)
            val postgresIterable = BasePostgresIterable<Map<String, Any>>(
                PreparedStatementHolderSupplier(
                    hds,
                    downloadTimeUseDiaryDataSql,
                    1024
                ) { ps ->
                    var index = 1
                    ps.setObject(index++, organizationId)
                    ps.setObject(index++, studyId)
                    ps.setString(index++, participantId)
                    ps.setArray(index, hds.connection.createArrayOf("UUID", submissionIds.toTypedArray()))
                }) { rs ->
                mapOf(
                    SUBMISSION_ID.name to setOf(rs.getString(SUBMISSION_ID.name)),
                    PARTICIPANT_ID.name to setOf(rs.getString(PARTICIPANT_ID.name)),
                    SUBMISSION_DATE.name to rs.getObject(SUBMISSION_DATE.name, OffsetDateTime::class.java)
                ) + convertTudJsonbToMap(rs, downloadType)
            }
            return PostgresDownloadWrapper(postgresIterable).withColumnAdvice(
                listOf(
                    SUBMISSION_ID.name,
                    PARTICIPANT_ID.name,
                    SUBMISSION_DATE.name
                ) + downloadType.downloadColumnTitles
            )
        } catch (ex: Exception) {
            throw error("Error: $ex")
        }
    }

    /* -------- Download Utils -------- */

    private fun convertTudJsonbToMap(
        rs: ResultSet,
        type: TimeUseDiaryDownloadDataType,
    ): Map<String, Set<String>> {
        val responses = extractTudResponsesFromResult(rs)
        return type.downloadColumnTitles.associateWith { kw ->
            responses.firstOrNull { it.question == kw }?.response ?: setOf("")
        }
    }

    private fun extractTudResponsesFromResult(resultSet: ResultSet): List<TimeUseDiaryResponse> {
        val submissionJson = resultSet.getObject(SUBMISSION.name, PGobject::class.java)
        return mapper.readValue(
            submissionJson.value,
            mapper.typeFactory.constructCollectionType(
                List::class.java,
                TimeUseDiaryResponse::class.java
            )
        )
    }

    /* -------- SQL helpers -------- */

    private fun executeInsertTimeUseDiarySql(
        connection: Connection,
        tudId: UUID,
        organizationId: UUID,
        studyId: UUID,
        participantId: String,
        responses: List<TimeUseDiaryResponse>,
        submissionDate: OffsetDateTime
    ) {
        connection.prepareStatement(insertTimeUseDiarySql).use { ps ->
            var index = 1
            ps.setObject(index++, tudId)
            ps.setObject(index++, organizationId)
            ps.setObject(index++, studyId)
            ps.setString(index++, participantId)
            ps.setObject(index++, submissionDate)
            ps.setObject(index, mapper.writeValueAsString(responses), Types.OTHER)
            ps.executeUpdate()
        }
    }

    private fun executeGetSubmissionByDateSql(
        connection: Connection,
        organizationId: UUID,
        studyId: UUID,
        participantId: String,
        startDate: OffsetDateTime,
        endDate: OffsetDateTime
    ): ResultSet {
        val preparedStatement = connection.prepareStatement(getSubmissionsByDateSql)
        var index = 1
        preparedStatement.setObject(index++, organizationId)
        preparedStatement.setObject(index++, studyId)
        preparedStatement.setString(index++, participantId)
        preparedStatement.setObject(index++, startDate)
        preparedStatement.setObject(index, endDate)
        return preparedStatement.executeQuery()
    }

    private fun executeGetStudyTUDSubmissionsByDateSql(
        connection: Connection,
        studyId: UUID,
        startDate: OffsetDateTime,
        endDate: OffsetDateTime
    ): ResultSet {
        val preparedStatement = connection.prepareStatement(getStudyTUDSubmissionsByDateSql)
        var index = 1
        preparedStatement.setObject(index++, studyId)
        preparedStatement.setObject(index++, startDate)
        preparedStatement.setObject(index, endDate)
        return preparedStatement.executeQuery()
    }

    private fun executeDownloadTimeUseDiaryData(
        connection: Connection,
        organizationId: UUID,
        studyId: UUID,
        participantId: String,
        timeUseDiaryIds: Set<UUID>
    ): ResultSet {
        val preparedStatement = connection.prepareStatement(downloadTimeUseDiaryDataSql)
        var index = 1
        preparedStatement.setObject(index++, organizationId)
        preparedStatement.setObject(index++, studyId)
        preparedStatement.setString(index++, participantId)
        preparedStatement.setArray(index, connection.createArrayOf("UUID", timeUseDiaryIds.toTypedArray()))
        return preparedStatement.executeQuery()
    }

    /**
     * SQL String to create a [java.sql.PreparedStatement] to submit a study response for a participant
     *
     * @param tudId             Identifies the Time Use Diary submission
     * @param organizationId    Identifies the organization from which to insert submissions
     * @param studyId           Identifies the study from which to insert submissions
     * @param participantId     Identifies the participant for whom to insert submissions
     * @param responses         List of survey responses for a Time Use Diary study
     */
    private val insertTimeUseDiarySql = """
        INSERT INTO ${TIME_USE_DIARY_SUBMISSIONS.name}
        VALUES ( ?, ?, ?, ?, ?, ? )
    """.trimIndent()

    /**
     * SQL String to create a [java.sql.PreparedStatement] to retrieve study submissions for a participant within a date range
     *
     * @param organizationId    Identifies the organization from which to retrieve submissions
     * @param studyId           Identifies the study from which to retrieve submissions
     * @param participantId     Identifies the participant for whom to retrieve submissions
     * @param startDate         Date that submissions must be submitted after or on
     * @param endDate           Date that submissions must be submitted before or on
     */
    private val getSubmissionsByDateSql = """
        SELECT ${SUBMISSION_ID.name}, ${SUBMISSION_DATE.name}
        FROM ${TIME_USE_DIARY_SUBMISSIONS.name}
        WHERE ${ORGANIZATION_ID.name} = ?
        AND ${STUDY_ID.name} = ?
        AND ${PARTICIPANT_ID.name} = ?
        AND ${SUBMISSION_DATE.name} BETWEEN ? AND ?
        ORDER BY ${SUBMISSION_DATE.name} ASC
    """.trimIndent()

    /**
     * SQL String to create a [java.sql.PreparedStatement] to retrieve all study submissions within a date range
     *
     * @param organizationId    Identifies the organization from which to retrieve submissions
     * @param studyId           Identifies the study from which to retrieve submissions
     * @param startDate         Date that submissions must be submitted after or on
     * @param endDate           Date that submissions must be submitted before or on
     */
    private val getStudyTUDSubmissionsByDateSql = """
        SELECT ${SUBMISSION_ID.name}, ${SUBMISSION_DATE.name}
        FROM ${TIME_USE_DIARY_SUBMISSIONS.name}
        WHERE ${STUDY_ID.name} = ?
        AND ${SUBMISSION_DATE.name} BETWEEN ? AND ?
        ORDER BY ${SUBMISSION_DATE.name} ASC
    """.trimIndent()

    /**
     * SQL String to create a [java.sql.PreparedStatement] to retrieve time use diary responses for download
     *
     * @param organizationId    Identifies the organization from which to retrieve submissions
     * @param studyId           Identifies the study from which to retrieve submissions
     * @param participantId     Identifies the participant for whom to retrieve submissions
     * @param timeUseDiaryIds   Identifies the time use diaries from which to retrieve submissions
     */
    private val downloadTimeUseDiaryDataSql = """
        SELECT ${SUBMISSION_ID.name}, ${PARTICIPANT_ID.name}, ${SUBMISSION_DATE.name}, ${SUBMISSION.name}
        FROM ${TIME_USE_DIARY_SUBMISSIONS.name}
        WHERE ${ORGANIZATION_ID.name} = ?
        AND ${STUDY_ID.name} = ?
        AND ${PARTICIPANT_ID.name} = ?
        AND ${SUBMISSION_ID.name} = ANY (?)
    """.trimIndent()

}
