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
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.TIME_USE_DIARY
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.OffsetDateTime
import java.util.*

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
            hds.connection.createStatement().execute(
                        insertTimeUseDiarySql(
                            tudId,
                            organizationId,
                            studyId,
                            participantId,
                            responses
                        )
                    )
        } catch(e: Exception) {
            logger.error("hds error: $e")
        }
        return tudId
    }

    override fun getSubmissionByDate(
        organizationId: UUID,
        studyId: UUID,
        participantId: String,
        startDate: OffsetDateTime,
        endDate: OffsetDateTime
    ): Map<LocalDate, Set<UUID>> {
        val submissionsByDate = mutableMapOf<LocalDate,MutableSet<UUID>>()
        // Use Calendar to convert response to user's timezone
        cal.timeZone = TimeZone.getTimeZone("GMT${startDate.offset.id}")
        /*
         * Query all submission UUIDs in date range then populate Map by iterating over ResultSet
         * If this method brings too much data into memory, it can be modified to query on a per-day basis
         */
        try {
            val (flavor, hds) = storageResolver.resolve(studyId)
            val result = hds.connection.createStatement().executeQuery(
                        getSubmissionByDateSql(
                            organizationId,
                            studyId,
                            participantId,
                            startDate,
                            endDate
                        )
                    )
            while (result.next()) {
                val currentDate = result.getDate(SUBMISSION_DATE.name, cal).toLocalDate()
                val currentUUID = UUID.fromString(result.getString(TUD_ID.name))
                // If new date encountered, initialize a new set in the map
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
        // TODO("Not yet implemented")
    }

    private fun insertTimeUseDiarySql(
        tudId: UUID,
        organizationId: UUID,
        studyId: UUID,
        participantId: String,
        responses: List<TimeUseDiaryResponse>
    ): String {
        return """
            INSERT INTO ${TIME_USE_DIARY.name} VALUES (
                '${tudId}',
                '${organizationId}',
                '${studyId}',
                '${participantId}',
                '${OffsetDateTime.now()}',
                '${objectMapper.writeValueAsString(responses)}')
            """.trimIndent()
    }

    private fun getSubmissionByDateSql(
        organizationId: UUID,
        studyId: UUID,
        participantId: String,
        startDate: OffsetDateTime,
        endDate: OffsetDateTime
    ): String {
        return """
                SELECT ${SUBMISSION_DATE.name}, ${TUD_ID.name} 
                FROM ${TIME_USE_DIARY.name}
                WHERE ${ORGANIZATION_ID.name} = '${organizationId}'
                AND ${STUDY_ID.name} = '${studyId}'
                AND ${PARTICIPANT_ID.name} = '${participantId}'
                AND ${SUBMISSION_DATE.name} BETWEEN '${startDate}' AND '${endDate}'
                ORDER BY ${SUBMISSION_DATE.name} ASC
            """.trimIndent()
    }
}