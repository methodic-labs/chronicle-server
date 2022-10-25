package com.openlattice.chronicle.services.timeusediary

import com.fasterxml.jackson.module.kotlin.readValue
import com.geekbeast.configuration.postgres.PostgresFlavor
import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.openlattice.chronicle.converters.TimeUseDiaryPostgresDownloadWrapper
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.participants.ParticipantStats
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.services.studies.StudyService
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.TIME_USE_DIARY_SUBMISSIONS
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.TIME_USE_DIARY_SUMMARIZED
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SUBMISSION
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SUBMISSION_DATE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SUBMISSION_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SUMMARY_DATA
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.timeusediary.*
import com.openlattice.chronicle.util.ChronicleServerUtil
import org.slf4j.LoggerFactory
import java.lang.IllegalArgumentException
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.time.LocalDate
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
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
        private val formatter = DateTimeFormatter.ofPattern("HH:mm") //24-hour format
    }

    override fun submitTimeUseDiary(
        connection: Connection,
        studyId: UUID,
        participantId: String,
        responses: List<TimeUseDiaryResponse>
    ): UUID {
        val timeUseDiaryId = idGenerationService.getNextId()
        val submissionDate = OffsetDateTime.now()
        executeInsertTimeUseDiarySql(
            connection,
            timeUseDiaryId,
            studyId,
            participantId,
            responses,
            submissionDate
        )
        updateParticipantStats(studyId, participantId, submissionDate)
        logger.info(
            "submitted time use diary responses ${ChronicleServerUtil.STUDY_PARTICIPANT}",
            studyId,
            participantId
        )
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
            tudFirstDate = currentStats?.tudFirstDate ?: submissionDate,
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

    override fun getStudyTUDSubmissionIdsByDate(
        studyId: UUID,
        startDate: OffsetDateTime,
        endDate: OffsetDateTime,
    ): Map<LocalDate, Set<UUID>> {
        try {
            val hds = storageResolver.getPlatformStorage(PostgresFlavor.VANILLA)
            val submissions = BasePostgresIterable(
                PreparedStatementHolderSupplier(
                    hds,
                    getStudyTUDSubmissionsByDateSql
                ) { ps ->
                    var index = 1
                    ps.setObject(index++, studyId)
                    ps.setObject(index++, startDate)
                    ps.setObject(index, endDate)
                }
            ) {
                val date = ResultSetAdapters.submissionDate(it)
                val id = ResultSetAdapters.submissionId(it)
                Pair(date.toLocalDate(), id)
            }.toList()

            val submissionsByDate: MutableMap<LocalDate, Set<UUID>> = mutableMapOf()
            submissions.forEach {
                submissionsByDate[it.first] = submissionsByDate.getOrDefault(it.first, setOf()) + setOf(it.second)
            }

            return submissionsByDate
        } catch (ex: SQLException) {
            logger.error("Error fetching submission ids by date: $ex")
            return mapOf()
        }
    }

    override fun getStudyTUDSubmissions(
        studyId: UUID,
        participantIds: Set<String>?,
        downloadType: TimeUseDiaryDownloadDataType,
        startDate: OffsetDateTime,
        endDate: OffsetDateTime,
    ): Iterable<List<Map<String, Any>>> {
        if (downloadType == TimeUseDiaryDownloadDataType.Summarized) {
            return getTimeUseDiarySummarizedData(studyId, participantIds, startDate, endDate)
        }
        try {
            val hds = storageResolver.getPlatformStorage(PostgresFlavor.VANILLA)
            val postgresIterable = BasePostgresIterable(
                PreparedStatementHolderSupplier(
                    hds,
                    getTimeUseDiaryDayOrNightTimeDataSql(participantIds),
                    1024
                ) { ps ->
                    var index = 0
                    ps.setObject(++index, studyId)
                    ps.setObject(++index, startDate)
                    ps.setObject(++index, endDate)
                    participantIds?.let {
                        ps.setArray(++index, PostgresArrays.createTextArray(hds.connection, it))
                    }
                }) { rs ->
                when (downloadType) {
                    TimeUseDiaryDownloadDataType.DayTime -> getDayTimeDataColumnMapping(rs)
                    TimeUseDiaryDownloadDataType.NightTime -> getNightTimeDataColumnMapping(rs)
                    else -> {
                        throw IllegalArgumentException("Unexpected data type: $downloadType")
                    }
                }
            }
            return TimeUseDiaryPostgresDownloadWrapper(postgresIterable).withColumnAdvice(downloadType.downloadColumnTitles.toList())
        } catch (ex: Exception) {
            logger.error("Error downloading TUD data", ex)
            return listOf()
        }
    }

    private fun getTimeUseDiarySummarizedData(
        studyId: UUID,
        participantIds: Set<String>?,
        startDate: OffsetDateTime,
        endDate: OffsetDateTime
    ): Iterable<List<Map<String, Any>>> {
        try {
            val hds = storageResolver.getPlatformStorage()
            val iterable = BasePostgresIterable(
                PreparedStatementHolderSupplier(
                    hds,
                    getSummarizedTimeUseDiarySql(participantIds),
                    1024
                ) { ps ->
                    var index = 0
                    ps.setObject(++index, studyId)
                    ps.setObject(++index, startDate)
                    ps.setObject(++index, endDate)
                    participantIds?.let {
                        ps.setArray(++index, PostgresArrays.createTextArray(hds.connection, it))
                    }
                }
            ) { getSummarizedDataColumnMapping(it) }

            return TimeUseDiaryPostgresDownloadWrapper(iterable).withColumnAdvice(TimeUseDiaryDownloadDataType.Summarized.downloadColumnTitles.toList())
        } catch (ex: Exception) {
            logger.info("Exception when downloading summarized data for study $studyId", ex)
            return listOf()
        }
    }

    /* -------- Download Utils -------- */

    private fun getDefaultColumnMapping(rs: ResultSet): Map<String, Any> {
        return mapOf(
            TimeUseDiaryColumTitles.PARTICIPANT_ID to rs.getString(PARTICIPANT_ID.name),
            TimeUseDiaryColumTitles.TIMESTAMP to rs.getObject(SUBMISSION_DATE.name, OffsetDateTime::class.java),
            TimeUseDiaryColumTitles.STUDY_ID to rs.getObject(STUDY_ID.name),
            TimeUseDiaryColumTitles.SUBMISSION_ID to rs.getObject(SUBMISSION_ID.name)
        )
    }

    private fun getSummarizedDataColumnMapping(rs: ResultSet): List<Map<String, Any>> {
        val defaultColumnMapping = getDefaultColumnMapping(rs)

        val values: List<TimeUseDiarySummarizedEntity> = mapper.readValue(rs.getString(SUMMARY_DATA.name))
        val valuesByVariableNames = values.associateBy { it.variable }

        val unmappedTitles = TimeUseDiaryDownloadDataType.Summarized.downloadColumnTitles - defaultColumnMapping.keys
        val summarizedValuesMapping = unmappedTitles.associateWith {
            valuesByVariableNames[it]?.value ?: ""
        }

        return listOf(defaultColumnMapping + summarizedValuesMapping)
    }

    private fun getDayTimeDataColumnMapping(rs: ResultSet): List<Map<String, Any>> {
        val result: MutableList<Map<String, Any>> = mutableListOf()

        val defaultColumnMapping = getDefaultColumnMapping(rs)

        val timeUseDiaryResponses: List<TimeUseDiaryResponse> = mapper.readValue(rs.getString(SUBMISSION.name))
        val responsesByStartDateTime =
            timeUseDiaryResponses.groupBy { it.startDateTime }.filter { it.key != null }.toSortedMap(compareBy { it })
        val responsesWithoutStartDateTime =
            timeUseDiaryResponses.filter { it.startDateTime == null }.associateBy { it.code }

        var counter = 0
        responsesByStartDateTime.forEach { (startDateTime, responses) ->
            val endDateTime = responses.first().endDateTime

            val responsesByCode: Map<String, TimeUseDiaryResponse> = responses.associateBy { it.code }

            val mappedColumns = mapOf(
                TimeUseDiaryColumTitles.ACTIVITY_DURATION to ChronoUnit.MINUTES.between(startDateTime, endDateTime),
                TimeUseDiaryColumTitles.ACTIVITY_COUNTER to ++counter,
                TimeUseDiaryColumTitles.ACTIVITY_START_TIME to startDateTime!!.toLocalTime().format(formatter),
                TimeUseDiaryColumTitles.ACTIVITY_END_TIME to endDateTime!!.toLocalTime().format(formatter)
            ) + defaultColumnMapping

            val additionalColumTitles = TimeUseDiaryDownloadDataType.DayTime.downloadColumnTitles - mappedColumns.keys
            val unmappedColumnTitles: MutableSet<String> = mutableSetOf()

            val timeRangeColumnMapping = additionalColumTitles.associateWith { title ->
                val questionCode = TimeUseDiaryColumTitles.columnTitleToQuestionCodeMap.getValue(title)
                if (!responsesByCode.containsKey(questionCode)) {
                    unmappedColumnTitles.add(title)
                    setOf()
                } else {
                    responsesByCode.getValue(questionCode).response
                }
            }

            val nonTimeRangeColumnMapping = unmappedColumnTitles.associateWith { title ->
                val questionCode = TimeUseDiaryColumTitles.columnTitleToQuestionCodeMap.getValue(title)
                responsesWithoutStartDateTime[questionCode]?.response ?: setOf()
            }

            result.add(mappedColumns + timeRangeColumnMapping + nonTimeRangeColumnMapping)
        }
        return result
    }

    private fun getNightTimeDataColumnMapping(rs: ResultSet): List<Map<String, Any>> {
        val defaultColumnMapping = getDefaultColumnMapping(rs)

        val responses: List<TimeUseDiaryResponse> = mapper.readValue(rs.getString(SUBMISSION.name))
        val responsesByCode = responses.associateBy { it.code }

        val zoneIdOfPrimaryActivity =
            responsesByCode.getValue(TimeUseDiaryQuestionCodes.PRIMARY_ACTIVITY).startDateTime!!.toZonedDateTime().zone
        val activityDate =
            LocalDate.parse(responsesByCode.getValue(TimeUseDiaryQuestionCodes.ACTIVITY_DATE).response.first())

        val activityDayStartTime =
            responsesByCode.getValue(TimeUseDiaryQuestionCodes.DAY_START_TIME).response.first() //HH:MM format
        val activityDayEndTime = responsesByCode.getValue(TimeUseDiaryQuestionCodes.DAY_END_TIME).response.first()

        //Attempt to read out bed time and wake up time
        val bedTimeBeforeActivityDay =
            responsesByCode[TimeUseDiaryQuestionCodes.BED_TIME_BEFORE_ACTIVITY_DAY]?.response?.first()
        val wakeUpTimeAfterActivityDay =
            responsesByCode[TimeUseDiaryQuestionCodes.WAKE_UP_TIME_AFTER_ACTIVITY_DAY]?.response?.first()

        //This is the legacy wake up time and is only present in the original TUDs
        val todayWakeUpTime = responsesByCode[TimeUseDiaryQuestionCodes.TODAY_WAKEUP_TIME]?.response?.first()

        //Make sure we got a valid combination
        check(
            ((bedTimeBeforeActivityDay == null) && (wakeUpTimeAfterActivityDay == null) && (todayWakeUpTime != null))
                    || ((bedTimeBeforeActivityDay != null) xor (wakeUpTimeAfterActivityDay != null))
        ) {
            "Either bed time before activity day or wake up time after activity day must be provided, but not both"
        }

        //Get the actual date times, by parsing out the times at the current date.
        
        val activityDayStartDateTime = LocalTime.parse(activityDayStartTime).atDate(activityDate)
        val activityDayEndDateTime = LocalTime.parse(activityDayEndTime).atDate(activityDate)
        val bedTimeBeforeActivityDayDateTime = if (bedTimeBeforeActivityDay != null) {
            LocalTime.parse(bedTimeBeforeActivityDay)
                .atDate(activityDate.minusDays(1))
                .atZone(zoneIdOfPrimaryActivity)
        } else null
        val wakeUpTimeAfterActivityDayDateTime = if (wakeUpTimeAfterActivityDay != null) {
            LocalTime.parse(wakeUpTimeAfterActivityDay)
                .atDate(activityDate.plusDays(1))
                .atZone(zoneIdOfPrimaryActivity)
        } else null

        //This bug remains for compatibility with legacy downloads.
        val todayWakeUpDateTime = if (todayWakeUpTime != null) {
            LocalTime
                .parse(todayWakeUpTime).atDate(LocalDate.now())
                .atZone(zoneIdOfPrimaryActivity)
        } else null

        val sleepColumnsSelection = if (todayWakeUpDateTime != null) {
            mapOf(
                TimeUseDiaryColumTitles.WAKE_UP_TODAY to (todayWakeUpDateTime.toLocalTime()?.format(formatter) ?: ""),
                TimeUseDiaryColumTitles.SLEEP_HOURS to ChronoUnit.HOURS.between(
                    activityDayEndDateTime,
                    todayWakeUpDateTime
                ),
            )
        } else {
            if (bedTimeBeforeActivityDayDateTime != null) {
                mapOf(
                    TimeUseDiaryColumTitles.BEDTIME_AFTER_ACTIVITY_DAY to (bedTimeBeforeActivityDayDateTime.toLocalTime()
                        ?.format(formatter) ?: ""),
                    TimeUseDiaryColumTitles.SLEEP_HOURS to ChronoUnit.HOURS.between(
                        bedTimeBeforeActivityDayDateTime,
                        activityDayStartDateTime
                    )
                )
            } else if (wakeUpTimeAfterActivityDayDateTime != null) {
                mapOf(
                    TimeUseDiaryColumTitles.WAKE_UP_TIME_AFTER_ACTIVITY_DAY to (wakeUpTimeAfterActivityDayDateTime?.toLocalTime()
                        ?.format(formatter) ?: ""),
                    TimeUseDiaryColumTitles.SLEEP_HOURS to ChronoUnit.HOURS.between(
                        activityDayEndDateTime,
                        wakeUpTimeAfterActivityDayDateTime
                    )
                )
            } else {
                throw IllegalStateException("This should never happen due null checks above, but is needed to make compiler happy.")
            }
        }

        val timeRangeMapping = mapOf(
            TimeUseDiaryColumTitles.WAKE_UP_YESTERDAY to activityDayStartDateTime.toLocalTime().format(formatter),
            TimeUseDiaryColumTitles.BED_TIME_YESTERDAY to activityDayEndDateTime.toLocalTime().format(formatter),
            TimeUseDiaryColumTitles.DAY_TIME_HOURS to ChronoUnit.HOURS.between(
                activityDayStartDateTime,
                activityDayEndDateTime
            ),
            TimeUseDiaryColumTitles.SLEEP_HOURS to ChronoUnit.HOURS.between(activityDayEndDateTime, todayWakeUpDateTime)
        ) + sleepColumnsSelection

        val additionalColumTitles =
            TimeUseDiaryDownloadDataType.NightTime.downloadColumnTitles - defaultColumnMapping.keys - timeRangeMapping.keys
        val additionalColumnMapping = additionalColumTitles.associateWith { title ->
            val code = TimeUseDiaryColumTitles.columnTitleToQuestionCodeMap.getValue(title)
            responsesByCode[code]?.response ?: setOf()
        }

        return listOf(defaultColumnMapping + additionalColumnMapping + timeRangeMapping)
    }

    /* -------- SQL helpers -------- */

    private fun executeInsertTimeUseDiarySql(
        connection: Connection,
        tudId: UUID,
        studyId: UUID,
        participantId: String,
        responses: List<TimeUseDiaryResponse>,
        submissionDate: OffsetDateTime
    ) {
        connection.prepareStatement(insertTimeUseDiarySql).use { ps ->
            var index = 1
            ps.setObject(index++, tudId)
            ps.setObject(index++, studyId)
            ps.setString(index++, participantId)
            ps.setObject(index++, submissionDate)
            ps.setString(index, mapper.writeValueAsString(responses))
            ps.executeUpdate()
        }
    }

    private fun executeGetSubmissionByDateSql(
        connection: Connection,
        studyId: UUID,
        participantId: String,
        startDate: OffsetDateTime,
        endDate: OffsetDateTime
    ): ResultSet {
        val preparedStatement = connection.prepareStatement(getSubmissionsByDateSql)
        var index = 1
        preparedStatement.setObject(index++, studyId)
        preparedStatement.setString(index++, participantId)
        preparedStatement.setObject(index++, startDate)
        preparedStatement.setObject(index, endDate)
        return preparedStatement.executeQuery()
    }

    private fun getTimeUseDiaryDayOrNightTimeDataSql(
        participantIds: Set<String>?
    ): String {
        var sql = """
             SELECT ${STUDY_ID.name}, ${SUBMISSION_ID.name}, ${PARTICIPANT_ID.name}, ${SUBMISSION_DATE.name}, ${SUBMISSION.name}
             FROM ${TIME_USE_DIARY_SUBMISSIONS.name}
             WHERE ${STUDY_ID.name} = ?
             AND ${SUBMISSION_DATE.name} >= ?
             AND ${SUBMISSION_DATE.name} < ?
        """.trimIndent()
        participantIds?.let {
            sql += " AND ${PARTICIPANT_ID.name} = ANY(?)"
        }
        return sql
    }

    private fun getSummarizedTimeUseDiarySql(
        participantIds: Set<String>?,
    ): String {
        var sql = """
            SELECT ${STUDY_ID.name}, ${SUBMISSION_ID.name}, ${PARTICIPANT_ID.name}, ${SUBMISSION_DATE.name}, ${SUMMARY_DATA.name}
            FROM ${TIME_USE_DIARY_SUMMARIZED.name}
            WHERE ${STUDY_ID.name} = ?
            AND ${SUBMISSION_DATE.name} >= ?
            AND ${SUBMISSION_DATE.name} < ?
        """.trimIndent()
        participantIds?.let {
            sql += " AND ${PARTICIPANT_ID.name} = ANY(?)"
        }
        return sql
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
        VALUES ( ?, ?, ?, ?, ?::jsonb )
    """.trimIndent()

    /**
     * SQL String to create a [java.sql.PreparedStatement] to retrieve study submissions for a participant within a date range
     *
     * @param studyId           Identifies the study from which to retrieve submissions
     * @param participantId     Identifies the participant for whom to retrieve submissions
     * @param startDate         Date that submissions must be submitted after or on
     * @param endDate           Date that submissions must be submitted before or on
     */
    private val getSubmissionsByDateSql = """
        SELECT ${SUBMISSION_ID.name}, ${SUBMISSION_DATE.name}
        FROM ${TIME_USE_DIARY_SUBMISSIONS.name}
        WHERE ${STUDY_ID.name} = ?
        AND ${PARTICIPANT_ID.name} = ?
        AND ${SUBMISSION_DATE.name} BETWEEN ? AND ?
        ORDER BY ${SUBMISSION_DATE.name} ASC
    """.trimIndent()

    /**
     * SQL String to create a [java.sql.PreparedStatement] to retrieve all study submissions within a date range
     *
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
     * @param studyId           Identifies the study from which to retrieve submissions
     * @param timeUseDiaryIds   Identifies the time use diaries from which to retrieve submissions
     */
    private val downloadTimeUseDiaryDataSql = """
        SELECT ${STUDY_ID.name}, ${SUBMISSION_ID.name}, ${PARTICIPANT_ID.name}, ${SUBMISSION_DATE.name}, ${SUBMISSION.name}
        FROM ${TIME_USE_DIARY_SUBMISSIONS.name}
        WHERE ${STUDY_ID.name} = ?
        AND ${SUBMISSION_DATE.name} BETWEEN ? AND ?
    """.trimIndent()

    /**
     * PreparedStatement bind order
     * 1) studyId
     * 2) startDate
     * 3) endDate
     */
    private val downloadSummarizedTimeUseDiaryDataSql = """
        SELECT ${STUDY_ID.name}, ${SUBMISSION_ID.name}, ${PARTICIPANT_ID.name}, ${SUBMISSION_DATE.name}, ${SUMMARY_DATA.name}
        FROM ${TIME_USE_DIARY_SUMMARIZED.name}
        WHERE ${STUDY_ID.name} = ?
        AND ${SUBMISSION_DATE.name} >= ?
        AND ${SUBMISSION_DATE.name} < ?
    """.trimIndent()
}
