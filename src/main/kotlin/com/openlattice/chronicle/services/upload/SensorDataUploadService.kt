package com.openlattice.chronicle.services.upload

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.geekbeast.configuration.postgres.PostgresFlavor
import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.postgres.PostgresColumnDefinition
import com.geekbeast.postgres.PostgresDatatype
import com.geekbeast.util.StopWatch
import com.google.common.util.concurrent.ListeningExecutorService
import com.google.common.util.concurrent.MoreExecutors
import com.openlattice.chronicle.participants.ParticipantStats
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.sensorkit.DeviceUsageData
import com.openlattice.chronicle.sensorkit.KeyboardMetricsData
import com.openlattice.chronicle.sensorkit.MessagesUsageData
import com.openlattice.chronicle.sensorkit.PhoneUsageData
import com.openlattice.chronicle.sensorkit.SensorDataSample
import com.openlattice.chronicle.sensorkit.SensorSourceDevice
import com.openlattice.chronicle.sensorkit.SensorType
import com.openlattice.chronicle.services.studies.StudyService
import com.openlattice.chronicle.storage.ChroniclePostgresTables
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.MAX_BIND_PARAMETERS
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.getMoveSql
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SOURCE_DEVICE_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.UPLOADED_AT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.UPLOAD_TYPE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.UPLOAD_DATA
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_CATEGORY
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_CATEGORY_WEB_DURATION
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_USAGE_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.BUNDLE_IDENTIFIER
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.DEVICE_MODEL
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.DEVICE_NAME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.DEVICE_SYSTEM_NAME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.DEVICE_USAGE_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.DEVICE_VERSION
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.END_DATE_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.KEYBOARD_METRICS_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.MESSAGES_USAGE_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.PATH_TYPING_SPEED
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.PHONE_USAGE_SENSOR_COLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.RECORDED_DATE_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.SAMPLE_DURATION
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.SAMPLE_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.SENSOR_TYPE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.SENTIMENT
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.SENTIMENT_EMOJI_COUNT
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.SENTIMENT_WORD_COUNT
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.START_DATE_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TEXT_INPUT_DURATION
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TEXT_INPUT_SOURCE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMEZONE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_ALTERED_WORDS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_AUTO_CORRECTIONS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_CALL_DURATION
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_DELETES
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_DRAGS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_EMOJIS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_INCOMING_CALLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_INCOMING_MESSAGES
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_INSERT_KEY_CORRECTIONS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_NEAR_KEY_CORRECTIONS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_OUTGOING_CALLS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_OUTGOING_MESSAGES
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_PATHS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_PATH_LENGTH
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_PATH_PAUSES
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_PATH_TIME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_PAUSES
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_RETRO_CORRECTIONS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_SCREEN_WAKES
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_SKIP_TOUCH_CORRECTIONS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_SPACE_CORRECTIONS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_SUBSTITUTION_CORRECTIONS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_TAPS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_TEST_HIT_CORRECTIONS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_TRANSPOSITION_CORRECTIONS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_TYPING_DURATION
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_TYPING_EPISODES
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_UNIQUE_CONTACTS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_UNLOCKS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_UNLOCK_DURATION
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TOTAL_WORDS
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TYPING_SPEED
import com.openlattice.chronicle.storage.RedshiftDataTables
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.IOS_SENSOR_DATA
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.buildMultilineInsertSensorEvents
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.util.ChronicleServerUtil
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.slf4j.event.Level
import java.security.InvalidParameterException
import java.sql.Connection
import java.sql.PreparedStatement
import java.time.LocalDate
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import kotlin.math.min

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class SensorDataUploadService(
    private val storageResolver: StorageResolver,
    private val studyService: StudyService
) : SensorDataUploadManager {

    companion object {
        private val logger = LoggerFactory.getLogger(SensorDataUploadService::class.java)
        internal val mapper: ObjectMapper = ObjectMappers.newJsonMapper()

        /**
         * 1. study id
         * 2. participant id
         * 3. upload data
         * 4. source device id (nullable)
         *
         */
        private val INSERT_UPLOAD_BUFFER_SQL = """
            INSERT INTO ${ChroniclePostgresTables.UPLOAD_BUFFER.name} (${STUDY_ID.name},${PARTICIPANT_ID.name},${UPLOAD_DATA.name}, ${UPLOADED_AT.name}, ${UPLOAD_TYPE.name}, ${SOURCE_DEVICE_ID.name}) 
            VALUES (?,?,?::jsonb,now(),'${UploadType.Ios.name}',?)
        """.trimIndent()
    }

    override fun upload(
        studyId: UUID,
        participantId: String,
        sourceDeviceId: String,
        data: List<SensorDataSample>
    ): Int {
        StopWatch(
            log = "Writing ${data.size} entites to Aurora for studyId = $studyId, participantId = $participantId ",
            level = Level.INFO,
            logger = logger,
        ).use {
            storageResolver.getPlatformStorage().connection.use { connection ->
                connection.prepareStatement(INSERT_UPLOAD_BUFFER_SQL).use { ps ->
                    ps.setObject(1, studyId)
                    ps.setString(2, participantId)
                    ps.setString(3, mapper.writeValueAsString(data))
                    ps.setString(4, sourceDeviceId)
                    ps.executeUpdate()
                }
            }
        }

//        updateParticipantStats(dataList, studyId, participantId)

        //Make sure device knows everything was flushed to db successfully.
        return data.size
    }


}

