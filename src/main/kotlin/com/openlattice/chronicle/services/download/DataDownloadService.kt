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
import com.openlattice.chronicle.storage.ChroniclePostgresTables
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.APP_USAGE_SURVEY
import com.openlattice.chronicle.storage.PostgresColumns
import com.openlattice.chronicle.storage.PostgresColumns.Companion.APP_USERS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APPLICATION_LABEL
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_PACKAGE_NAME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.DEVICE_USAGE_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.INTERACTION_TYPE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.KEYBOARD_METRICS_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.MESSAGES_USAGE_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.PHONE_USAGE_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.RECORDED_DATE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.SENSOR_TYPE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.SHARED_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMESTAMP
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMEZONE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.USERNAME
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.CHRONICLE_USAGE_EVENTS
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.IOS_SENSOR_DATA
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
            SELECT $CHRONICLE_USAGE_EVENTS_COLS FROM ${CHRONICLE_USAGE_EVENTS.name} WHERE 
             ${STUDY_ID.name} = ? AND ${PARTICIPANT_ID.name} = ?
        """.trimIndent()

        private val APP_USAGE_SURVEY_COLS = APP_USAGE_SURVEY.columns.joinToString { it.name }

        private const val FETCH_SIZE = 32768

        fun associateString(rs: ResultSet, pcd: PostgresColumnDefinition) = pcd.name to rs.getString(pcd.name)
        fun associateOffsetDatetimeWithTimezone(
            rs: ResultSet,
            timezoneColumn: PostgresColumnDefinition,
            timestampColumn: PostgresColumnDefinition
        ): Pair<String, OffsetDateTime> {
            val zoneId = ZoneId.of(rs.getString(timezoneColumn.name) ?: OutputConstants.DEFAULT_TIMEZONE)
            val odt = rs.getObject(timestampColumn.name, OffsetDateTime::class.java)
            return timestampColumn.name to odt.toInstant().atZone(zoneId).toOffsetDateTime()
        }

        fun associateObject(rs: ResultSet, pcd: PostgresColumnDefinition, clazz: Class<*>) =
            pcd.name to rs.getObject(pcd.name, clazz)

        private fun getSensorDateTimeFilterClause(startDateTime: OffsetDateTime?, endDateTime: OffsetDateTime?): String {
            var result = ""
            startDateTime?.let {
                result += " AND ${RECORDED_DATE.name} >= ?"
            }
            endDateTime?.let {
                result += " AND ${RECORDED_DATE.name} < ?"
            }
            return result
        }

        private fun getSensorDataColsAndSql(
            sensors: Set<SensorType>,
            startDateTime: OffsetDateTime?,
            endDateTime: OffsetDateTime?
        ): Pair<Set<PostgresColumnDefinition>, String> {
            val mapping = mapOf(
                SensorType.phoneUsage to PHONE_USAGE_SENSOR_COLS,
                SensorType.keyboardMetrics to KEYBOARD_METRICS_SENSOR_COLS,
                SensorType.deviceUsage to DEVICE_USAGE_SENSOR_COLS,
                SensorType.messagesUsage to MESSAGES_USAGE_SENSOR_COLS
            )

            val cols = SHARED_SENSOR_COLS - PARTICIPANT_ID - STUDY_ID + sensors.flatMap { mapping.getValue(it) }.toSet()
            val values = cols.joinToString(",") { it.name }

            val sql = """
                SELECT $values FROM ${IOS_SENSOR_DATA.name}
                WHERE ${STUDY_ID.name} = ? 
                AND ${PARTICIPANT_ID.name} = Any(?) 
                AND ${SENSOR_TYPE.name} = Any(?) 
                ${getSensorDateTimeFilterClause(startDateTime, endDateTime)})
            """.trimIndent()

            return Pair(cols, sql)
        }

        private fun getUsageEventsDateTimeFilterClause(startDateTime: OffsetDateTime?, endDateTime: OffsetDateTime?): String{
            var result = ""
            startDateTime?.let {
                result += " AND ${TIMESTAMP.name} >= ?"
            }
            endDateTime?.let {
                result += " AND ${TIMESTAMP.name} < ?"
            }

            return result
        }

        private fun getAppUsageDateTimeFilterClause(startDateTime: OffsetDateTime?, endDateTime: OffsetDateTime?): String {
            var result = ""
            startDateTime?.let {
                result += " AND ${TIMESTAMP.name} >= ?"
            }
            endDateTime?.let {
                result += " AND ${TIMESTAMP.name} < ?"
            }
            return result
        }

        private fun getUsageEventsSql(startDateTime: OffsetDateTime?, endDateTime: OffsetDateTime?): String {
            return """
                SELECT $CHRONICLE_USAGE_EVENTS_COLS 
                FROM ${CHRONICLE_USAGE_EVENTS.name} 
                WHERE ${STUDY_ID.name} = ? AND ${PARTICIPANT_ID.name} = Any (?)
                ${getUsageEventsDateTimeFilterClause(startDateTime, endDateTime)}
            """.trimIndent()
        }

        private fun getAppUsageSurveyDataSql(startDateTime: OffsetDateTime?, endDateTime: OffsetDateTime?): String {
            return """
                SELECT $APP_USAGE_SURVEY_COLS
                FROM ${APP_USAGE_SURVEY.name}
                WHERE ${STUDY_ID.name} = ?
                AND ${PARTICIPANT_ID.name} = Any (?)
                ${getAppUsageDateTimeFilterClause(startDateTime, endDateTime)}
            """.trimIndent()
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
        startDateTime: OffsetDateTime?,
        endDateTime: OffsetDateTime?
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
        val colsAndSql = getSensorDataColsAndSql(sensors, startDateTime, endDateTime)
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
                startDateTime?.let {
                    ps.setObject(++index, startDateTime)
                }
                endDateTime?.let {
                    ps.setObject(++index, endDateTime)
                }
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
        startDateTime: OffsetDateTime?,
        endDateTime: OffsetDateTime?
    ): Iterable<Map<String, Any>> {

        val hds = storageResolver.getPlatformStorage()
        val iterable = BasePostgresIterable<Map<String, Any>>(
            PreparedStatementHolderSupplier(
                hds,
                getAppUsageSurveyDataSql(startDateTime, endDateTime),
                FETCH_SIZE
            ) { ps ->
                var index = 0
                ps.setObject(++index, studyId)
                ps.setArray(++index, PostgresArrays.createTextArray(hds.connection, participantIds))
                startDateTime?.let {
                    ps.setObject(++index, it)
                }
                endDateTime?.let {
                    ps.setObject(++index, it)
                }
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
        startDateTime: OffsetDateTime?,
        endDateTime: OffsetDateTime?
    ): Iterable<Map<String, Any>> {
        val (_, hds) = storageResolver.resolveAndGetFlavor(studyId)
        val pgIter = BasePostgresIterable<Map<String, Any>>(
            PreparedStatementHolderSupplier(
                hds,
                getUsageEventsSql(startDateTime, endDateTime),
                FETCH_SIZE
            ) { ps ->
                var index = 0
                ps.setString(++index, studyId.toString())
                ps.setArray(++index, PostgresArrays.createTextArray(hds.connection, participantIds))
                startDateTime?.let {
                    ps.setObject(++index, it)
                }
                endDateTime?.let {
                    ps.setObject(++index, it)
                }
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
