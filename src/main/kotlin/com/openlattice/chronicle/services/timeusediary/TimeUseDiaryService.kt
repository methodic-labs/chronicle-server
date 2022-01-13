package com.openlattice.chronicle.services.timeusediary

import com.fasterxml.jackson.databind.ObjectMapper
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.tud.TimeUseDiaryDownloadDataType
import com.openlattice.chronicle.tud.TimeUseDiaryResponse
import com.openlattice.chronicle.storage.PostgresColumns.Companion.TUD_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SUBMISSION_DATE
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.TIME_USE_DIARY_SUBMISSION
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.ResultSet
import java.time.LocalDate
import java.util.*
import java.sql.Types
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneOffset

/**
 * @author Andrew Carter andrew@openlattice.com
 */
class TimeUseDiaryService(
    private val storageResolver: StorageResolver
) : TimeUseDiaryManager {

    companion object {
        private val logger = LoggerFactory.getLogger(TimeUseDiaryService::class.java)
        private val objectMapper = ObjectMapper()
        private val cal = Calendar.getInstance()
    }

    override fun submitTimeUseDiary(
        organizationId: UUID,
        studyId: UUID,
        participantId: String,
        responses: List<TimeUseDiaryResponse>
    ): UUID {
        val tudId = UUID.randomUUID()
        try {
            val (flavor, hds) = storageResolver.resolve(studyId)
            hds.connection.use { conn ->
                executeInsertTimeUseDiarySql(
                    conn,
                    tudId,
                    organizationId,
                    studyId,
                    participantId,
                    responses
                )
            }
        } catch(e: Exception) {
            logger.error("hds error: $e")
        }
        return tudId
    }

    override fun getSubmissionByDate(
        organizationId: UUID,
        studyId: UUID,
        participantId: String,
        startDate: LocalDateTime,
        endDate: LocalDateTime,
        zoneOffset: ZoneOffset
    ): Map<LocalDate, Set<UUID>> {
        val submissionsByDate = mutableMapOf<LocalDate,MutableSet<UUID>>()
        // Use Calendar to convert response to user's timezone
        cal.timeZone = TimeZone.getTimeZone("GMT${zoneOffset.id}")
        try {
            val (flavor, hds) = storageResolver.resolve(studyId)
            val result = hds.connection.use { connection ->
                executeGetSubmissionByDateSql(
                    connection,
                    organizationId,
                    studyId,
                    participantId,
                    startDate.atOffset(zoneOffset),
                    endDate.atOffset(zoneOffset)
                )
            }
            while (result.next()) {
                val currentDate = result.getDate(SUBMISSION_DATE.name, cal).toLocalDate()
                val currentUUID = UUID.fromString(result.getString(TUD_ID.name))
                if (submissionsByDate[currentDate].isNullOrEmpty()) {
                    submissionsByDate[currentDate] = mutableSetOf(currentUUID)
                } else {
                    submissionsByDate[currentDate]!!.add(currentUUID)
                }
            }
        } catch (e: Exception) {
            logger.error("hds error: $e")
        }
        return submissionsByDate
    }

    override fun downloadTimeUseDiaryData(
        organizationId: UUID,
        studyId: UUID,
        participantId: String,
        type: TimeUseDiaryDownloadDataType,
        submissionsIds: Set<UUID>
    ) {
        TODO("Not yet implemented")
    }

    /* -------- SQL helpers -------- */

    private fun executeInsertTimeUseDiarySql(
        connection: Connection,
        tudId: UUID,
        organizationId: UUID,
        studyId: UUID,
        participantId: String,
        responses: List<TimeUseDiaryResponse>
    ) {
        val preparedStatement = connection.prepareStatement(insertTimeUseDiarySql)
        var index = 1
        preparedStatement.setObject(index++, tudId)
        preparedStatement.setObject(index++, organizationId)
        preparedStatement.setObject(index++, studyId)
        preparedStatement.setString(index++, participantId)
        preparedStatement.setObject(index, objectMapper.writeValueAsString(responses), Types.OTHER)
        preparedStatement.execute()
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

    /**
     * SQL String to create a [java.sql.PreparedStatement] to submit a study response for a participant
     *
     * @param tudId             Identifies the Time Use Diary submission
     * @param organizationId    Identifies the organization from which to retrieve submissions
     * @param studyId           Identifies the study from which to retrieve submissions
     * @param participantId     Identifies the participant for whom to retrieve submissions
     * @param responses         List of survey responses for a Time Use Diary study
     */
    private val insertTimeUseDiarySql = """
            INSERT INTO ${TIME_USE_DIARY_SUBMISSION.name} 
            VALUES ( ?, ?, ?, ?, '${LocalDateTime.now()}', ? )
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
                SELECT ${SUBMISSION_DATE.name}, ${TUD_ID.name} 
                FROM ${TIME_USE_DIARY_SUBMISSION.name}
                WHERE ${ORGANIZATION_ID.name} = ?
                AND ${STUDY_ID.name} = ?
                AND ${PARTICIPANT_ID.name} = ?
                AND ${SUBMISSION_DATE.name} BETWEEN ? AND ?
                ORDER BY ${SUBMISSION_DATE.name} ASC
            """.trimIndent()
}