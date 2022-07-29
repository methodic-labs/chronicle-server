package com.openlattice.chronicle.services.download

import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.PostgresColumnDefinition
import com.geekbeast.postgres.PostgresDatatype
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.openlattice.chronicle.constants.OutputConstants
import com.openlattice.chronicle.constants.ParticipantDataType
import com.openlattice.chronicle.converters.PostgresDownloadWrapper
import com.openlattice.chronicle.sensorkit.SensorType
import com.openlattice.chronicle.services.surveys.SurveysService
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.APP_USAGE_SURVEY
import com.openlattice.chronicle.storage.PostgresColumns
import com.openlattice.chronicle.storage.PostgresColumns.Companion.APP_USERS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APPLICATION_LABEL
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_DATETIME_START
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_PACKAGE_NAME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_TIMEZONE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.DEVICE_USAGE_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.INTERACTION_TYPE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.KEYBOARD_METRICS_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.MESSAGES_USAGE_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.PHONE_USAGE_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.RECORDED_DATE_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.SENSOR_TYPE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.SHARED_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMESTAMP
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMEZONE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.USERNAME
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.CHRONICLE_USAGE_EVENTS
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.IOS_SENSOR_DATA
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.PREPROCESSED_USAGE_EVENTS
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.util.ChronicleServerUtil
import org.slf4j.LoggerFactory
import java.sql.ResultSet
import java.time.OffsetDateTime
import java.time.ZoneId
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class DataDownloadService(
    private val storageResolver: StorageResolver,
) : DataDownloadManager {
    companion object {
        private val logger = LoggerFactory.getLogger(DataDownloadService::class.java)
        private val CHRONICLE_USAGE_EVENTS_DEFS = CHRONICLE_USAGE_EVENTS.columns.map { it.name }
        private val CHRONICLE_USAGE_EVENTS_COLS = CHRONICLE_USAGE_EVENTS_DEFS.joinToString(",")
        private val CHRONICLE_USAGE_EVENT_SQL = """
            SELECT $CHRONICLE_USAGE_EVENTS_COLS 
            FROM ${CHRONICLE_USAGE_EVENTS.name} 
            WHERE ${STUDY_ID.name} = ?
            AND ${PARTICIPANT_ID.name} = ANY(?)
            AND ${TIMESTAMP.name} >= ?
            AND ${TIMESTAMP.name} < ?
        """.trimIndent()

        private val PREPROCESSED_DATA_COLS = PREPROCESSED_USAGE_EVENTS.columns.map { it.name }
        private val PREPROCESSED_DATA_COLS_STR = PREPROCESSED_DATA_COLS.joinToString(", ")

        /**
         * PreparedStatement binding
         * 1) studyId
         * 2) participant ids
         * 3) startDate
         * 4) endDate
         */
        private val PREPROCESSED_DATA_SQL = """
            SELECT $PREPROCESSED_DATA_COLS_STR
            FROM ${PREPROCESSED_USAGE_EVENTS.name}
            WHERE ${STUDY_ID.name} = ?
            AND ${PARTICIPANT_ID.name} = ANY(?)
            AND ${APP_DATETIME_START.name} >= ?
            AND ${APP_DATETIME_START.name} < ?
        """.trimIndent()

        private val APP_USAGE_SURVEY_COLS = APP_USAGE_SURVEY.columns.joinToString { it.name }

        val APP_USAGE_SURVEY_SQL = """
             SELECT $APP_USAGE_SURVEY_COLS
             FROM ${APP_USAGE_SURVEY.name}
             WHERE ${STUDY_ID.name} = ?
             AND ${PARTICIPANT_ID.name} = ANY(?)
             AND ${TIMESTAMP.name} >= ?
             AND ${TIMESTAMP.name} < ?
        """.trimIndent()

        private const val FETCH_SIZE = 32768

        fun associateString(rs: ResultSet, pcd: PostgresColumnDefinition) = pcd.name to rs.getString(pcd.name)
        fun associateInteger(rs: ResultSet, pcd: PostgresColumnDefinition) = pcd.name to rs.getInt(pcd.name)
        fun associateDouble(rs: ResultSet, pcd: PostgresColumnDefinition) = pcd.name to rs.getDouble(pcd.name)
        fun associateOffsetDatetimeWithTimezone(
            rs: ResultSet,
            timezoneColumn: PostgresColumnDefinition,
            timestampColumn: PostgresColumnDefinition
        ): Pair<String, Any> {
            val zoneId = ZoneId.of(rs.getString(timezoneColumn.name) ?: OutputConstants.DEFAULT_TIMEZONE)
            val odt = rs.getObject(timestampColumn.name, OffsetDateTime::class.java)
            return if(odt == null ) timestampColumn.name to ""
            else timestampColumn.name to odt.toInstant().atZone(zoneId).toOffsetDateTime()
        }

        fun associateObject(rs: ResultSet, pcd: PostgresColumnDefinition, clazz: Class<*>) =
            pcd.name to rs.getObject(pcd.name, clazz)

        private fun getSensorDataColsAndSql(
            sensors: Set<SensorType>,
        ): Pair<Set<PostgresColumnDefinition>, String> {
            val mapping = mapOf(
                SensorType.phoneUsage to PHONE_USAGE_SENSOR_COLS,
                SensorType.keyboardMetrics to KEYBOARD_METRICS_SENSOR_COLS,
                SensorType.deviceUsage to DEVICE_USAGE_SENSOR_COLS,
                SensorType.messagesUsage to MESSAGES_USAGE_SENSOR_COLS
            )

            val cols = SHARED_SENSOR_COLS + sensors.flatMap { mapping.getValue(it) }.toSet()
            val values = cols.joinToString(",") { it.name }

            val sql = """
                SELECT $values FROM ${IOS_SENSOR_DATA.name}
                WHERE ${STUDY_ID.name} = ? 
                AND ${PARTICIPANT_ID.name} = Any(?) 
                AND ${SENSOR_TYPE.name} = Any(?) 
                AND ${RECORDED_DATE_TIME.name} >= ?
                AND ${RECORDED_DATE_TIME.name} < ?
            """.trimIndent()

            return Pair(cols, sql)
        }

    }

    private fun getParticipantDataHelper(
        studyId: UUID,
        participantId: String,
        dataType: ParticipantDataType
    ): Iterable<Map<String, Any>> {
        val (flavor, hds) = storageResolver.resolveAndGetFlavor(studyId)
        val pgIter = BasePostgresIterable<Map<String, Any>>(
            PreparedStatementHolderSupplier(
                hds,
                CHRONICLE_USAGE_EVENT_SQL,
                FETCH_SIZE
            ) { ps ->
                ps.setString(1, studyId.toString())
                ps.setString(2, participantId)
            }) { rs ->
            mapOf(
                associateString(rs, STUDY_ID),
                associateString(rs, PARTICIPANT_ID),
                associateString(rs, APP_PACKAGE_NAME),
                associateString(rs, INTERACTION_TYPE),
                associateOffsetDatetimeWithTimezone(rs, TIMEZONE, TIMESTAMP),
                associateString(rs, TIMEZONE),
                associateString(rs, USERNAME),
                associateString(rs, APPLICATION_LABEL)
            )
        }

        return PostgresDownloadWrapper(pgIter).withColumnAdvice(CHRONICLE_USAGE_EVENTS.columns.map { it.name })
    }

    override fun getParticipantData(
        studyId: UUID,
        participantId: String,
        dataType: ParticipantDataType,
        token: String
    ): Iterable<Map<String, Any>> {
        return getParticipantDataHelper(
            studyId,
            participantId,
            dataType
        )
    }

    override fun getParticipantsSensorData(
        studyId: UUID,
        participantIds: Set<String>,
        sensors: Set<SensorType>,
        startDateTime: OffsetDateTime,
        endDateTime: OffsetDateTime
    ): Iterable<Map<String, Any>> {
        if (sensors.isEmpty()) {
            logger.warn(
                "study does not have any configured sensors" + ChronicleServerUtil.STUDY_PARTICIPANTS,
                studyId,
                participantIds
            )
            return listOf()
        }

        val (_, hds) = storageResolver.resolveAndGetFlavor(studyId)
        val colsAndSql = getSensorDataColsAndSql(sensors)
        val cols = colsAndSql.first
        val sql = colsAndSql.second

        val iterable = BasePostgresIterable<Map<String, Any>>(
            PreparedStatementHolderSupplier(
                hds,
                sql,
                FETCH_SIZE
            ) { ps ->
                var index = 0
                ps.setString(++index, studyId.toString())
                ps.setArray(++index, PostgresArrays.createTextArray(hds.connection, participantIds))
                ps.setArray(++index, PostgresArrays.createTextArray(hds.connection, sensors.map { it.name }))
                ps.setObject(++index, startDateTime)
                ps.setObject(++index, endDateTime)
            }
        ) { rs ->
            cols.associate { col ->
                when (col.datatype) {
                    PostgresDatatype.TIMESTAMPTZ -> associateOffsetDatetimeWithTimezone(rs, TIMEZONE, col)
                    else -> associateString(rs, col)
                }
            }
        }

        return PostgresDownloadWrapper(iterable).withColumnAdvice(cols.map { it.name })
    }

    override fun getParticipantsAppUsageSurveyData(
        studyId: UUID,
        participantIds: Set<String>,
        startDateTime: OffsetDateTime,
        endDateTime: OffsetDateTime
    ): Iterable<Map<String, Any>> {

        val hds = storageResolver.getPlatformStorage()
        val iterable = BasePostgresIterable<Map<String, Any>>(
            PreparedStatementHolderSupplier(
                hds,
                APP_USAGE_SURVEY_SQL,
                FETCH_SIZE
            ) { ps ->
                var index = 0
                ps.setObject(++index, studyId)
                ps.setArray(++index, PostgresArrays.createTextArray(hds.connection, participantIds))
                ps.setObject(++index, startDateTime)
                ps.setObject(++index, endDateTime)
            }
        ) { rs ->
            mapOf(
                associateObject(rs, STUDY_ID, UUID::class.java),
                associateString(rs, PARTICIPANT_ID),
                associateString(rs, APPLICATION_LABEL),
                associateString(rs, APP_PACKAGE_NAME),
                associateOffsetDatetimeWithTimezone(rs, TIMEZONE, TIMESTAMP),
                associateString(rs, TIMEZONE),
                associateString(rs, APP_USERS)
            )
        }

        return PostgresDownloadWrapper(iterable).withColumnAdvice(APP_USAGE_SURVEY.columns.map { it.name })
    }

    override fun getParticipantsUsageEventsData(
        studyId: UUID,
        participantIds: Set<String>,
        startDateTime: OffsetDateTime,
        endDateTime: OffsetDateTime
    ): Iterable<Map<String, Any>> {
        val (_, hds) = storageResolver.resolveAndGetFlavor(studyId)
        val pgIter = BasePostgresIterable<Map<String, Any>>(
            PreparedStatementHolderSupplier(
                hds,
                CHRONICLE_USAGE_EVENT_SQL,
                FETCH_SIZE
            ) { ps ->
                var index = 0
                ps.setString(++index, studyId.toString())
                ps.setArray(++index, PostgresArrays.createTextArray(hds.connection, participantIds))
                ps.setObject(++index, startDateTime)
                ps.setObject(++index, endDateTime)
            }) { rs ->
            mapOf(
                associateString(rs, STUDY_ID),
                associateString(rs, PARTICIPANT_ID),
                associateString(rs, APP_PACKAGE_NAME),
                associateString(rs, INTERACTION_TYPE),
                associateOffsetDatetimeWithTimezone(rs, TIMEZONE, TIMESTAMP),
                associateString(rs, TIMEZONE),
                associateString(rs, USERNAME),
                associateString(rs, APPLICATION_LABEL)
            )
        }

        return PostgresDownloadWrapper(pgIter).withColumnAdvice(CHRONICLE_USAGE_EVENTS.columns.map { it.name })
    }

    override fun getPreprocessedUsageEventsData(
        studyId: UUID,
        participantIds: Set<String>,
        startDateTime: OffsetDateTime,
        endDateTime: OffsetDateTime
    ): Iterable<Map<String, Any>> {
        val (_, hds) = storageResolver.resolveAndGetFlavor(studyId)

        val resultSetAwareCols = PREPROCESSED_USAGE_EVENTS.columns.map {
            val name = it.name.replace("\"", "")
            PostgresColumnDefinition(name, it.datatype)
        }
        val pgIterable = BasePostgresIterable<Map<String, Any>>(
            PreparedStatementHolderSupplier(
                hds,
                PREPROCESSED_DATA_SQL,
                FETCH_SIZE
            ) { ps ->
                var index = 0
                ps.setString(++index, studyId.toString())
                ps.setArray(++index, PostgresArrays.createTextArray(hds.connection, participantIds))
                ps.setObject(++index, startDateTime)
                ps.setObject(++index, endDateTime)
            }
        ) { rs ->
            resultSetAwareCols.associate {
                when(it.datatype) {
                    PostgresDatatype.TEXT -> associateString(rs, it)
                    PostgresDatatype.TEXT_128 -> associateString(rs, it)
                    PostgresDatatype.TEXT_256 -> associateString(rs, it)
                    PostgresDatatype.TIMESTAMPTZ -> associateOffsetDatetimeWithTimezone(rs, APP_TIMEZONE, it)
                    PostgresDatatype.TEXT_UUID -> associateString(rs, it)
                    PostgresDatatype.INTEGER -> associateInteger(rs, it)
                    PostgresDatatype.DOUBLE -> associateDouble(rs, it)
                    else -> throw RuntimeException("Invalid column type: ${it.datatype}")
                }
            }
        }

        return PostgresDownloadWrapper(pgIterable).withColumnAdvice(resultSetAwareCols.map { it.name })
    }

    override fun getQuestionnaireResponses(
        studyId: UUID,
        questionnaireId: UUID
    ): Iterable<Map<String, Any>> {
        val cols = listOf(
            PostgresColumns.PARTICIPANT_ID,
            PostgresColumns.COMPLETED_AT,
            PostgresColumns.QUESTION_TITLE,
            PostgresColumns.RESPONSES
        )
        val iterable = BasePostgresIterable<Map<String, Any>>(
            PreparedStatementHolderSupplier(
                hds = storageResolver.getPlatformStorage(),
                SurveysService.GET_QUESTIONNAIRE_SUBMISSIONS_SQL,
                FETCH_SIZE
            ) { ps ->
                ps.setObject(1, studyId)
                ps.setObject(2, questionnaireId)
            }
        ) { rs ->
            mapOf(
                associateString(rs, PostgresColumns.PARTICIPANT_ID),
                associateObject(rs, PostgresColumns.COMPLETED_AT, OffsetDateTime::class.java),
                associateString(rs, PostgresColumns.QUESTION_TITLE),
                associateString(rs, PostgresColumns.RESPONSES)
            )
        }

        return PostgresDownloadWrapper(iterable).withColumnAdvice(cols.map { it.name })
    }
}
