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

        private val semaphore = Semaphore(10)
        private val RS_BATCH_SIZE = (65536 / IOS_SENSOR_DATA.columns.size)
        private val executor: ListeningExecutorService =
            MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1))


        /**
         * 1. study id
         * 2. participant id
         * 3. upload data
         * 4. source device id (nullable)
         *
         */
        private val INSERT_UPLOAD_BUFFER_SQL = """
            INSERT INTO ${ChroniclePostgresTables.UPLOAD_BUFFER.name} (${STUDY_ID.name},${PARTICIPANT_ID.name},${UPLOAD_DATA.name}, ${UPLOADED_AT.name}, ${UPLOAD_TYPE.name}, ${SOURCE_DEVICE_ID.name}) 
            VALUES (?,?,?::jsonb,now(),${UploadType.Ios.name},?)
        """.trimIndent()
    }

    init {
        executor.execute {
            while (true) {
                moveToEventStorage()
                Thread.sleep(5 * 60 * 1000)
            }
        }
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

    private fun moveToEventStorage() {
        try {
            if (!semaphore.tryAcquire()) return
            logger.info("Moving ios data from aurora to event storage.")
            val queueEntriesByFlavor: MutableMap<PostgresFlavor, MutableList<SensorDataRow>> = mutableMapOf()
            storageResolver.getPlatformStorage().connection.use { platform ->
                platform.autoCommit = false
                platform.createStatement().use { stmt ->
                    stmt.executeQuery(getMoveSql(128, UploadType.Ios)).use { rs ->
                        while (rs.next()) {
                            val sensorDataSamples = ResultSetAdapters.sensorDataSamples(rs)
                            val (flavor, _) = storageResolver.resolveAndGetFlavor(sensorDataSamples.studyId)
                            queueEntriesByFlavor.getOrPut(flavor) { mutableListOf() }
                                .addAll(sensorDataSamples.toSensorDataRows())
                        }
                    }

                    logger.info("Total number of entries for redshift: ${(queueEntriesByFlavor[PostgresFlavor.REDSHIFT] ?: listOf()).size}")
                    logger.info("Total number of entries for postgres: ${(queueEntriesByFlavor[PostgresFlavor.VANILLA] ?: listOf()).size}")
                    queueEntriesByFlavor.forEach { (postgresFlavor, sensorDataEntries) ->
                        if (sensorDataEntries.isEmpty()) return@forEach
                        when (postgresFlavor) {
                            PostgresFlavor.REDSHIFT -> writeToEventStorage(
                                storageResolver.getEventStorageWithFlavor(PostgresFlavor.REDSHIFT),
                                sensorDataEntries,
                                false
                            )
                            PostgresFlavor.VANILLA -> writeToEventStorage(
                                storageResolver.getEventStorageWithFlavor(PostgresFlavor.VANILLA),
                                sensorDataEntries,
                                true
                            )
                        }
                    }
                }
                platform.commit()
                platform.autoCommit = true
            }
            logger.info("Successfully moved ios data to event storage.")
        } catch (ex: Exception) {
            logger.info("Unable to move data from aurora to redshift.", ex)
            throw ex
        } finally {
            semaphore.release()
        }
    }

    private fun writeToEventStorage(
        hds: HikariDataSource,
        data: List<SensorDataRow>,
        includeOnConflict: Boolean
    ): Int {
        return StopWatch(
            log = "writing ${data.size} entries to event storage.",
            level = Level.INFO,
            logger = logger
        ).use {
            val w = hds.connection.use { connection ->
                connection.autoCommit = false
                val insertBatchSize = min(data.size, RS_BATCH_SIZE)

                logger.info("Preparing primary insert statement (sensor data) with batch size $insertBatchSize")
                val insertSql = RedshiftDataTables.buildMultilineInsertUsageEvents(
                    insertBatchSize,
                    includeOnConflict
                )

                val pps = connection.prepareStatement(insertSql)

                val dr = data.size % RS_BATCH_SIZE

                val fps = if (data.size > RS_BATCH_SIZE && dr != 0) {
                    logger.info("Preparing secondary insert statement with batch size $dr")
                    connection.prepareStatement(buildMultilineInsertSensorEvents(dr, includeOnConflict))
                } else {
                    pps
                }

                val s = try {
                    data.chunked(insertBatchSize).forEach { sensorDataRows ->
                        val ps = if (insertBatchSize == sensorDataRows.size) {
                            pps
                        } else {
                            fps
                        }
                        var offset = 0
                        data.forEach {
                            val studyId = it.studyId
                            val participantId = it.participantId
                            val sourceDeviceId = it.sourceDeviceId

                            logger.info(
                                "Moving ${data.size} items to even for storage (ios) " + ChronicleServerUtil.STUDY_PARTICIPANT_DATASOURCE,
                                studyId,
                                participantId,
                                sourceDeviceId
                            )

                            writeSensorDataToRedshift(ps, offset, studyId, participantId, it.sensorType, it.row)
                            offset += IOS_SENSOR_DATA.columns.size
                        }
                        if (ps === pps)
                            ps.addBatch()
                    }

                    //We only call fps.executeBatch() if they ended up different objects.
                    pps.executeBatch().sum() + if (pps !== fps) {
                        fps.executeUpdate()
                    } else {
                        0
                    }
                } finally {
                    if (pps !== fps) fps.close()
                    pps.close()
                }

                connection.commit()
                connection.autoCommit = true
                s
            }
            //Process all the participant updates. Being lazy hear since I don't have them batched.
            data.forEach {
                updateParticipantStats(
                    it.studyId,
                    it.participantId,
                    mapOf(it.sensorType to listOf(it.row))
                )
            }
            w
        }
    }


    private fun writeSensorDataToRedshift(
        ps: PreparedStatement,
        offset: Int,
        studyId: UUID,
        participantId: String,
        sensorType: SensorType,
        dataColumns: List<SensorDataColumn>
    ) {
        ps.setString(offset + RedshiftDataTables.getInsertSensorDataColumnIndex(STUDY_ID), studyId.toString())
        ps.setString(offset + RedshiftDataTables.getInsertSensorDataColumnIndex(PARTICIPANT_ID), participantId)
        ps.setString(offset + RedshiftDataTables.getInsertSensorDataColumnIndex(SENSOR_TYPE), sensorType.name)

        dataColumns.forEach { dataColumn ->
            val col = dataColumn.col
            val index = offset + dataColumn.colIndex
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
    }

    private fun updateParticipantStats(
        studyId: UUID,
        participantId: String,
        data: Map<SensorType, List<List<SensorDataColumn>>>
    ) {
        val currentStats = studyService.getParticipantStats(studyId, participantId)
        val dates: MutableSet<OffsetDateTime> =
            data.values.asSequence().flatten().flatten().filter { it.col == RECORDED_DATE_TIME }
                .map { it.value as OffsetDateTime }.toMutableSet()
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
            iosLastPing = OffsetDateTime.now(),
            iosFirstDate = minDate,
            iosLastDate = maxDate,
            tudFirstDate = currentStats?.tudFirstDate,
            tudLastDate = currentStats?.tudLastDate,
            tudUniqueDates = currentStats?.tudUniqueDates ?: setOf()
        )
        studyService.insertOrUpdateParticipantStats(statsUpdate)
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
    val nulCols =
        nullifyCols(DEVICE_USAGE_SENSOR_COLS + MESSAGES_USAGE_SENSOR_COLS + KEYBOARD_METRICS_SENSOR_COLS - TOTAL_UNIQUE_CONTACTS)

    data.forEach {
        val phoneUsageData: PhoneUsageData = SensorDataUploadService.mapper.readValue(it.data)
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
    val defaultNullCols =
        nullifyCols(PHONE_USAGE_SENSOR_COLS + MESSAGES_USAGE_SENSOR_COLS + KEYBOARD_METRICS_SENSOR_COLS)

    data.forEach sample@{ sample ->
        val deviceUsageData: DeviceUsageData = SensorDataUploadService.mapper.readValue(sample.data)
        val appCategories: Set<String> = deviceUsageData.appUsage.keys + deviceUsageData.webUsage.keys
        val summaryCols = listOf(
            SensorDataColumn(TOTAL_UNLOCK_DURATION, deviceUsageData.totalUnlockDuration),
            SensorDataColumn(TOTAL_SCREEN_WAKES, deviceUsageData.totalScreenWakes),
            SensorDataColumn(TOTAL_UNLOCKS, deviceUsageData.totalUnlocks)
        )

        if (appCategories.isEmpty()) {
            val cols = nullifyCols(
                setOf(
                    APP_CATEGORY,
                    APP_USAGE_TIME,
                    TEXT_INPUT_DURATION,
                    TEXT_INPUT_SOURCE,
                    BUNDLE_IDENTIFIER,
                    APP_CATEGORY_WEB_DURATION
                )
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
                val cols = nullifyCols(
                    setOf(
                        TEXT_INPUT_SOURCE,
                        TEXT_INPUT_DURATION,
                        APP_USAGE_TIME,
                        APP_CATEGORY,
                        BUNDLE_IDENTIFIER
                    )
                ).toMutableList()
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
        val keyboardMetricsData: KeyboardMetricsData = SensorDataUploadService.mapper.readValue(sample.data)
        val sentiments =
            keyboardMetricsData.emojiCountBySentiment.keys + keyboardMetricsData.wordCountBySentiment.keys

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
    val nulCols =
        nullifyCols(DEVICE_USAGE_SENSOR_COLS + PHONE_USAGE_SENSOR_COLS + KEYBOARD_METRICS_SENSOR_COLS - TOTAL_UNIQUE_CONTACTS)

    data.forEach {
        val messagesUsageData: MessagesUsageData = SensorDataUploadService.mapper.readValue(it.data)
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
    val device: SensorSourceDevice = SensorDataUploadService.mapper.readValue(dataSample.device)

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

internal data class SensorDataColumn(
    val col: PostgresColumnDefinition,
    val value: Any?
) {
    val colIndex: Int = RedshiftDataTables.getInsertSensorDataColumnIndex(col)
}

internal data class MappedSensorData(
    val studyId: UUID,
    val participantId: String,
    val data: Map<SensorType, List<List<SensorDataColumn>>>,
    val uploadedAt: OffsetDateTime,
    val sourceDeviceId: String,
)

internal data class SensorDataRow(
    val studyId: UUID,
    val participantId: String,
    val sensorType: SensorType,
    val row: List<SensorDataColumn>,
    val uploadedAt: OffsetDateTime,
    val sourceDeviceId: String,
)

data class SensorDataEntries(
    val studyId: UUID,
    val participantId: String,
    val data: List<SensorDataSample>,
    val uploadedAt: OffsetDateTime,
    val sourceDeviceId: String,
) {
    internal fun toSensorDataRows(): List<SensorDataRow> {
        return mapSensorDataToStorage(data).flatMap { (sensorType, rows) ->
            rows.map { row ->
                SensorDataRow(
                    studyId,
                    participantId,
                    sensorType,
                    row,
                    uploadedAt,
                    sourceDeviceId
                )
            }
        }
    }

    internal fun toMappedData(): MappedSensorData {
        return MappedSensorData(studyId, participantId, mapSensorDataToStorage(data), uploadedAt, sourceDeviceId)
    }
}
