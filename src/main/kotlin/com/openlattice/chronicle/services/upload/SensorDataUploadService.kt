package com.openlattice.chronicle.services.upload

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.postgres.PostgresColumnDefinition
import com.geekbeast.postgres.PostgresDatatype
import com.geekbeast.util.StopWatch
import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.participants.ParticipantStats
import com.openlattice.chronicle.sensorkit.DeviceUsageData
import com.openlattice.chronicle.sensorkit.KeyboardMetricsData
import com.openlattice.chronicle.sensorkit.MessagesUsageData
import com.openlattice.chronicle.sensorkit.PhoneUsageData
import com.openlattice.chronicle.sensorkit.SensorDataSample
import com.openlattice.chronicle.sensorkit.SensorSourceDevice
import com.openlattice.chronicle.sensorkit.SensorType
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.services.studies.StudyService
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
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.util.ChronicleServerUtil
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.slf4j.event.Level
import java.time.LocalDate
import java.time.OffsetDateTime
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class SensorDataUploadService(
        private val storageResolver: StorageResolver,
        private val enrollmentManager: EnrollmentManager,
        private val studyService: StudyService
) : SensorDataUploadManager {

    companion object {
        private val logger = LoggerFactory.getLogger(SensorDataUploadService::class.java)

        private val mapper: ObjectMapper = ObjectMappers.newJsonMapper()

        private val SENSOR_DATA_COLUMNS = IOS_SENSOR_DATA.columns.joinToString(",") { it.name }
        private val SENSOR_DATA_COLS_BIND = IOS_SENSOR_DATA.columns.joinToString(",") { "?" }

        // TODO: specify bind order
        /**
         * PreparedStatement bind order
         * 1)
         */
        private val INSERT_SENSOR_DATA_SQL = """
            INSERT INTO ${IOS_SENSOR_DATA.name}($SENSOR_DATA_COLUMNS) VALUES ($SENSOR_DATA_COLS_BIND)
        """.trimIndent()
    }

    override fun upload(studyId: UUID, participantId: String, sourceDeviceId: String, data: List<SensorDataSample>): Int {

        StopWatch(
                log = "logging ${data.size} entries for ${ChronicleServerUtil.STUDY_PARTICIPANT_DATASOURCE}",
                level = Level.INFO,
                logger = logger,
                studyId,
                participantId,
                sourceDeviceId
        ).use {
            try {
                val (_, hds) = storageResolver.resolveAndGetFlavor(studyId)
                val status = enrollmentManager.getParticipationStatus(studyId, participantId)
                if (ParticipationStatus.NOT_ENROLLED == status) {
                    logger.warn(
                            "participant is not enrolled, ignoring sensor data upload" + ChronicleServerUtil.STUDY_PARTICIPANT_DATASOURCE,
                            studyId,
                            participantId,
                            sourceDeviceId
                    )
                    return 0
                }
                val deviceEnrolled = enrollmentManager.isKnownDatasource(studyId, participantId, sourceDeviceId)

                if (!deviceEnrolled) {
                    logger.error(
                            "data source not found, ignoring sensor data upload" + ChronicleServerUtil.STUDY_PARTICIPANT_DATASOURCE,
                            studyId,
                            participantId,
                            sourceDeviceId
                    )
                    return 0
                }

                logger.info(
                        "attempting to upload sensor data" + ChronicleServerUtil.STUDY_PARTICIPANT_DATASOURCE,
                        studyId,
                        participantId,
                        sourceDeviceId
                )

                val mappedData: Map<SensorType, List<List<SensorDataColumn>>> = mapSensorDataToStorage(data)

                return writeSensorDataToRedshift(hds, studyId, participantId, mappedData)
            } catch (ex: Exception) {
                logger.error(
                        "error writing sensor data" + ChronicleServerUtil.STUDY_PARTICIPANT_DATASOURCE,
                        studyId,
                        participantId,
                        sourceDeviceId,
                        ex
                )
                return 0
            }
        }
    }

    private fun mapSensorDataToStorage(data: List<SensorDataSample>): Map<SensorType, List<List<SensorDataColumn>>> {
        return data.groupBy { it.sensor }.mapValues {
            val mappedValues = when (it.key) {
                SensorType.phoneUsage -> mapPhoneUsageData(it.value)
                SensorType.deviceUsage -> mapDeviceUsageData(it.value)
                SensorType.keyboardMetrics -> mapKeyboardMetricsData(it.value)
                SensorType.messagesUsage -> mapMessagesUsageData(it.value)
            }
            return mapOf(it.key to mappedValues)
        }
    }

    private fun mapPhoneUsageData(data: List<SensorDataSample>): List<List<SensorDataColumn>> {
        val result: MutableList<List<SensorDataColumn>> = mutableListOf()
        val nulCols = nullifyCols(DEVICE_USAGE_SENSOR_COLS + MESSAGES_USAGE_SENSOR_COLS + KEYBOARD_METRICS_SENSOR_COLS - TOTAL_UNIQUE_CONTACTS)

        data.forEach {
            val phoneUsageData: PhoneUsageData = mapper.readValue(it.data)
            val cols = mutableListOf(
                    SensorDataColumn(TOTAL_INCOMING_CALLS, phoneUsageData.totalIncomingCalls),
                    SensorDataColumn(TOTAL_OUTGOING_CALLS, phoneUsageData.totalOutgoingCalls),
                    SensorDataColumn(TOTAL_CALL_DURATION, phoneUsageData.totalPhoneDuration),
                    SensorDataColumn(TOTAL_UNIQUE_CONTACTS, phoneUsageData.totalUniqueContacts)
            )
            cols.addAll(mapSharedColumns(it))
            cols.addAll(nulCols)
            result.add(cols)
        }

        return result
    }

    private fun mapDeviceUsageData(data: List<SensorDataSample>): List<List<SensorDataColumn>> {
        val result: MutableList<List<SensorDataColumn>> = mutableListOf()
        val defaultNullCols = nullifyCols(PHONE_USAGE_SENSOR_COLS + MESSAGES_USAGE_SENSOR_COLS + KEYBOARD_METRICS_SENSOR_COLS)

        data.forEach sample@{ sample ->
            val deviceUsageData: DeviceUsageData = mapper.readValue(sample.data)
            val appCategories: Set<String> = deviceUsageData.appUsage.keys + deviceUsageData.webUsage.keys
            val summaryCols = listOf(
                    SensorDataColumn(TOTAL_UNLOCK_DURATION, deviceUsageData.totalUnlockDuration),
                    SensorDataColumn(TOTAL_SCREEN_WAKES, deviceUsageData.totalScreenWakes),
                    SensorDataColumn(TOTAL_UNLOCKS, deviceUsageData.totalUnlocks)
            )

            if (appCategories.isEmpty()) {
                val cols = nullifyCols(
                        setOf(APP_CATEGORY, APP_USAGE_TIME, TEXT_INPUT_DURATION, TEXT_INPUT_SOURCE, BUNDLE_IDENTIFIER, APP_CATEGORY_WEB_DURATION)
                ).toMutableList()
                cols.addAll(summaryCols)
                cols.addAll(defaultNullCols)
                cols.addAll(mapSharedColumns(sample))
                result.add(cols)
                return@sample
            }

            appCategories.forEach categories@{ category ->
                val appUsages = deviceUsageData.appUsage.getOrDefault(category, listOf())
                val webUsage = deviceUsageData.webUsage[category]

                if (appUsages.isEmpty()) {
                    val cols = nullifyCols(setOf(TEXT_INPUT_SOURCE, TEXT_INPUT_DURATION, APP_USAGE_TIME, APP_CATEGORY, BUNDLE_IDENTIFIER)).toMutableList()
                    cols.add(SensorDataColumn(APP_CATEGORY_WEB_DURATION, webUsage))
                    cols.addAll(summaryCols)
                    cols.addAll(defaultNullCols)
                    cols.addAll(mapSharedColumns(sample))
                    result.add(cols)

                    return@categories
                }

                appUsages.forEach usage@{ usage ->
                    if (usage.textInputSessions.isEmpty()) {
                        val cols = mutableListOf(
                                SensorDataColumn(TEXT_INPUT_SOURCE, null),
                                SensorDataColumn(TEXT_INPUT_DURATION, null),
                                SensorDataColumn(APP_USAGE_TIME, usage.usageTime),
                                SensorDataColumn(APP_CATEGORY, category),
                                SensorDataColumn(BUNDLE_IDENTIFIER, usage.bundleIdentifier),
                                SensorDataColumn(APP_CATEGORY_WEB_DURATION, webUsage),
                        )
                        cols.addAll(summaryCols)
                        cols.addAll(defaultNullCols)
                        cols.addAll(mapSharedColumns(sample))
                        result.add(cols)

                        return@usage
                    }

                    usage.textInputSessions.forEach { (inputSource, duration) ->
                        val cols = mutableListOf(
                                SensorDataColumn(TEXT_INPUT_SOURCE, inputSource),
                                SensorDataColumn(TEXT_INPUT_DURATION, duration),
                                SensorDataColumn(APP_USAGE_TIME, usage.usageTime),
                                SensorDataColumn(APP_CATEGORY, category),
                                SensorDataColumn(BUNDLE_IDENTIFIER, usage.bundleIdentifier),
                                SensorDataColumn(APP_CATEGORY_WEB_DURATION, webUsage)
                        )
                        cols.addAll(defaultNullCols)
                        cols.addAll(summaryCols)
                        cols.addAll(mapSharedColumns(sample))
                        result.add(cols)
                    }
                }
            }
        }
        return result
    }

    private fun mapKeyboardMetricsData(data: List<SensorDataSample>): List<List<SensorDataColumn>> {
        val result: MutableList<MutableList<SensorDataColumn>> = mutableListOf()
        val nullCols = nullifyCols(PHONE_USAGE_SENSOR_COLS + MESSAGES_USAGE_SENSOR_COLS + DEVICE_USAGE_SENSOR_COLS)

        data.forEach { sample ->
            val keyboardMetricsData: KeyboardMetricsData = mapper.readValue(sample.data)
            val sentiments = keyboardMetricsData.emojiCountBySentiment.keys + keyboardMetricsData.wordCountBySentiment.keys

            if (sentiments.isEmpty()) {
                val cols = mapDefaultKeyboardMetricsCols(keyboardMetricsData).toMutableList()
                cols.add(SensorDataColumn(SENTIMENT, null))
                cols.add(SensorDataColumn(SENTIMENT_WORD_COUNT, null))
                cols.add(SensorDataColumn(SENTIMENT_EMOJI_COUNT, null))
                cols.addAll(mapSharedColumns(sample))
                cols.addAll(nullCols)
                result.add(cols)
                return@forEach
            }
            sentiments.forEach { sentiment ->
                val cols = mapDefaultKeyboardMetricsCols(keyboardMetricsData).toMutableList()
                cols.add(SensorDataColumn(SENTIMENT, sentiment))
                cols.add(SensorDataColumn(SENTIMENT_WORD_COUNT, keyboardMetricsData.wordCountBySentiment[sentiment]))
                cols.add(SensorDataColumn(SENTIMENT_EMOJI_COUNT, keyboardMetricsData.emojiCountBySentiment[sentiment]))
                cols.addAll(mapSharedColumns(sample))
                cols.addAll(nullCols)
                result.add(cols)
            }
        }

        return result
    }

    private fun mapDefaultKeyboardMetricsCols(data: KeyboardMetricsData): List<SensorDataColumn> {
        return listOf(
                SensorDataColumn(TOTAL_WORDS, data.totalWords),
                SensorDataColumn(TOTAL_ALTERED_WORDS, data.totalAlteredWords),
                SensorDataColumn(TOTAL_TAPS, data.totalTaps),
                SensorDataColumn(TOTAL_DRAGS, data.totalDrags),
                SensorDataColumn(TOTAL_DELETES, data.totalDeletes),
                SensorDataColumn(TOTAL_EMOJIS, data.totalEmojis),
                SensorDataColumn(TOTAL_PATHS, data.totalPaths),
                SensorDataColumn(TOTAL_PATH_LENGTH, data.totalPathLength),
                SensorDataColumn(TOTAL_PATH_TIME, data.totalPathTime),
                SensorDataColumn(TOTAL_AUTO_CORRECTIONS, data.totalAutoCorrections),
                SensorDataColumn(TOTAL_SPACE_CORRECTIONS, data.totalSpaceCorrections),
                SensorDataColumn(TOTAL_TRANSPOSITION_CORRECTIONS, data.totalTranspositionCorrections),
                SensorDataColumn(TOTAL_INSERT_KEY_CORRECTIONS, data.totalInsertKeyCorrections),
                SensorDataColumn(TOTAL_RETRO_CORRECTIONS, data.totalRetroCorrections),
                SensorDataColumn(TOTAL_SKIP_TOUCH_CORRECTIONS, data.totalSkipTouchCorrections),
                SensorDataColumn(TOTAL_NEAR_KEY_CORRECTIONS, data.totalNearKeyCorrections),
                SensorDataColumn(TOTAL_SUBSTITUTION_CORRECTIONS, data.totalSubstitutionCorrections),
                SensorDataColumn(TOTAL_TEST_HIT_CORRECTIONS, data.totalHitTestCorrections),
                SensorDataColumn(TOTAL_TYPING_DURATION, data.totalTypingDuration),
                SensorDataColumn(TOTAL_PATH_PAUSES, data.totalPathPauses),
                SensorDataColumn(TOTAL_PAUSES, data.totalPauses),
                SensorDataColumn(TOTAL_TYPING_EPISODES, data.totalTypingEpisodes),
                SensorDataColumn(TYPING_SPEED, data.typingSpeed),
                SensorDataColumn(PATH_TYPING_SPEED, data.pathTypingSpeed)
        )
    }

    private fun mapMessagesUsageData(data: List<SensorDataSample>): List<List<SensorDataColumn>> {
        val result: MutableList<List<SensorDataColumn>> = mutableListOf()
        val nulCols = nullifyCols(DEVICE_USAGE_SENSOR_COLS + PHONE_USAGE_SENSOR_COLS + KEYBOARD_METRICS_SENSOR_COLS - TOTAL_UNIQUE_CONTACTS)

        data.forEach {
            val messagesUsageData: MessagesUsageData = mapper.readValue(it.data)
            val cols = mutableListOf(
                    SensorDataColumn(TOTAL_INCOMING_MESSAGES, messagesUsageData.totalIncomingMessages),
                    SensorDataColumn(TOTAL_OUTGOING_MESSAGES, messagesUsageData.totalOutgoingMessages),
                    SensorDataColumn(TOTAL_UNIQUE_CONTACTS, messagesUsageData.totalUniqueContacts)
            )
            cols.addAll(mapSharedColumns(it))
            cols.addAll(nulCols)
            result.add(cols)
        }

        return result
    }

    private fun mapSharedColumns(dataSample: SensorDataSample): List<SensorDataColumn> {
        val device: SensorSourceDevice = mapper.readValue(dataSample.device)

        return listOf(
                SensorDataColumn(SAMPLE_ID, dataSample.id.toString()),
                SensorDataColumn(SENSOR_TYPE, dataSample.sensor.name),
                SensorDataColumn(SAMPLE_DURATION, dataSample.duration),
                SensorDataColumn(RECORDED_DATE_TIME, dataSample.dateRecorded),
                SensorDataColumn(START_DATE_TIME, dataSample.startDate),
                SensorDataColumn(END_DATE_TIME, dataSample.endDate),
                SensorDataColumn(TIMEZONE, dataSample.timezone),
                SensorDataColumn(DEVICE_VERSION, device.systemVersion),
                SensorDataColumn(DEVICE_NAME, device.name),
                SensorDataColumn(DEVICE_MODEL, device.model),
                SensorDataColumn(DEVICE_SYSTEM_NAME, device.name),
        )
    }

    private fun nullifyCols(cols: Set<PostgresColumnDefinition>): List<SensorDataColumn> {
        return cols.map { SensorDataColumn(it, null) }
    }

    private fun writeSensorDataToRedshift(
            hds: HikariDataSource,
            studyId: UUID,
            participantId: String,
            data: Map<SensorType, List<List<SensorDataColumn>>>
    ): Int {

        return hds.connection.use { connection ->
            try {
                connection.autoCommit = false
                val wc = connection.prepareStatement(INSERT_SENSOR_DATA_SQL).use { ps ->
                    ps.setString(RedshiftDataTables.getInsertSensorDataColumnIndex(STUDY_ID), studyId.toString())
                    ps.setString(RedshiftDataTables.getInsertSensorDataColumnIndex(PARTICIPANT_ID), participantId)

                    data.forEach { (sensorType, dataRows) ->
                        ps.setString(RedshiftDataTables.getInsertSensorDataColumnIndex(SENSOR_TYPE), sensorType.name)

                        dataRows.forEach { dataColumns ->
                            dataColumns.forEach { dataColumn ->
                                val col = dataColumn.col
                                val index = dataColumn.colIndex
                                val value = dataColumn.value

                                if (value == null) {
                                    ps.setObject(index, null)
                                } else {
                                    when (col.datatype) {
                                        PostgresDatatype.TEXT -> ps.setString(index, value as String)
                                        PostgresDatatype.DOUBLE -> ps.setDouble(index, value as Double)
                                        else -> ps.setObject(index, value)
                                    }
                                }
                            }

                            ps.addBatch()
                        }
                    }
                    ps.executeBatch().sum()
                }

                updateParticipantStats(studyId, participantId, data)
                connection.commit()
                connection.autoCommit = true
                return@use wc
            } catch (ex: Exception) {
                connection.rollback()
                throw ex
            }
        }
    }

    private fun updateParticipantStats(studyId: UUID, participantId: String, data: Map<SensorType, List<List<SensorDataColumn>>>) {
        val currentStats = studyService.getParticipantStats(studyId, participantId)
        val dates: MutableSet<OffsetDateTime> = data.values.asSequence().flatten().flatten().filter { it.col == RECORDED_DATE_TIME }.map { it.value as OffsetDateTime }.toMutableSet()
        currentStats?.iosLastDate?.let {
            dates += it
        }
        currentStats?.iosFirstDate?.let {
            dates += it
        }

        val currentUniqueDates = currentStats?.iosUniqueDates ?: setOf()
        val uniqueDates: Set<LocalDate> = dates.map { it.toLocalDate() }.toSet() + currentUniqueDates

        val minDate = dates.stream().min(OffsetDateTime::compareTo).get()
        val maxDate = dates.stream().max(OffsetDateTime::compareTo).get()

        val statsUpdate = ParticipantStats(
            studyId = studyId,
            participantId = participantId,
            androidFirstDate = currentStats?.androidFirstDate,
            androidLastDate = currentStats?.androidLastDate,
            androidUniqueDates = currentStats?.androidUniqueDates ?: setOf(),
            iosUniqueDates = uniqueDates,
            iosFirstDate = minDate,
            iosLastDate = maxDate,
            tudFirstDate = currentStats?.tudFirstDate,
            tudLastDate = currentStats?.tudLastDate,
            tudUniqueDates = currentStats?.tudUniqueDates ?: setOf()
        )
        studyService.insertOrUpdateParticipantStats(statsUpdate)
    }
}

private data class SensorDataColumn(
        val col: PostgresColumnDefinition,
        val value: Any?
) {
    val colIndex: Int = RedshiftDataTables.getInsertSensorDataColumnIndex(col)
}