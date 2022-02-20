package com.openlattice.chronicle.services.download

import com.geekbeast.postgres.PostgresColumnDefinition
import com.geekbeast.postgres.PostgresDatatype
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.openlattice.chronicle.constants.OutputConstants
import com.openlattice.chronicle.constants.ParticipantDataType
import com.openlattice.chronicle.converters.PostgresDownloadWrapper
import com.openlattice.chronicle.sensorkit.SensorType
import com.openlattice.chronicle.services.surveys.SurveysManager
import com.openlattice.chronicle.services.surveys.SurveysService
import com.openlattice.chronicle.storage.PostgresColumns
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APPLICATION_LABEL
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_PACKAGE_NAME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.DEVICE_USAGE_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.INTERACTION_TYPE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.KEYBOARD_METRICS_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.MESSAGES_USAGE_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.PHONE_USAGE_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.SENSOR_TYPE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.SHARED_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMESTAMP
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMEZONE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.USERNAME
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.CHRONICLE_USAGE_EVENTS
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.IOS_SENSOR_DATA
import com.openlattice.chronicle.storage.StorageResolver
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

        private fun getSensorTypeFilterClause(sensors: Set<SensorType>): String {
            return sensors.joinToString(" OR ") { "${SENSOR_TYPE.name} = ?" }
        }

        fun getSensorDataColsAndSql(sensors: Set<SensorType>): Pair<Set<PostgresColumnDefinition>, String> {
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
                WHERE ${STUDY_ID.name} = ? AND ${PARTICIPANT_ID.name} = ? AND (${getSensorTypeFilterClause(sensors)})
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
                32768
            ) { ps ->
                ps.setString(1, studyId.toString())
                ps.setString(2, participantId)
            }) { rs ->
            mapOf(
                associateString(rs, ORGANIZATION_ID),
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

    private fun getSensorDataHelper(
        studyId: UUID,
        participantId: String,
        sensors: Set<SensorType>
    ): Iterable<Map<String, Any>> {
        val (_, hds) = storageResolver.resolveAndGetFlavor(studyId)
        val colsAndSql = getSensorDataColsAndSql(sensors)
        val cols = colsAndSql.first
        val sql = colsAndSql.second

        val iterable = BasePostgresIterable<Map<String, Any>>(
            PreparedStatementHolderSupplier(
                hds,
                sql,
                32768
            ) { ps ->
                var index = 0
                ps.setString(++index, studyId.toString())
                ps.setString(++index, participantId)
                sensors.forEach {
                    ps.setString(++index, it.name)
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

    private fun getQuestionnaireResponsesHelper(studyId: UUID, questionnaireId: UUID): Iterable<Map<String, Any>> {
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
                32768
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

    override fun getParticipantSensorData(
        studyId: UUID,
        participantId: String,
        sensors: Set<SensorType>
    ): Iterable<Map<String, Any>> {
        return getSensorDataHelper(studyId, participantId, sensors)
    }

    override fun getQuestionnaireResponses(
        studyId: UUID,
        questionnaireId: UUID
    ): Iterable<Map<String, Any>> {
        return getQuestionnaireResponsesHelper(studyId, questionnaireId)
    }
}
