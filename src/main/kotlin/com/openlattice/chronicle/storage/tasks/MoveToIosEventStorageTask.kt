package com.openlattice.chronicle.storage.tasks

import com.fasterxml.jackson.module.kotlin.readValue
import com.geekbeast.configuration.postgres.PostgresFlavor
import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.PostgresColumnDefinition
import com.geekbeast.postgres.PostgresDatatype
import com.geekbeast.tasks.HazelcastFixedRateTask
import com.geekbeast.tasks.Task
import com.geekbeast.util.StopWatch
import com.google.common.util.concurrent.ListeningExecutorService
import com.google.common.util.concurrent.MoreExecutors
import com.openlattice.chronicle.participants.ParticipantStats
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.sensorkit.*
import com.openlattice.chronicle.services.studies.StudyManager
import com.openlattice.chronicle.services.upload.*
import com.openlattice.chronicle.storage.ChroniclePostgresTables
import com.openlattice.chronicle.storage.RedshiftColumns
import com.openlattice.chronicle.storage.RedshiftDataTables
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.IOS_SENSOR_DATA
import com.openlattice.chronicle.storage.odtFromUsageEventColumn
import com.openlattice.chronicle.util.ChronicleServerUtil
import com.zaxxer.hikari.HikariDataSource
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import org.slf4j.event.Level
import java.security.InvalidParameterException
import java.sql.PreparedStatement
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import kotlin.math.min

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class MoveToIosEventStorageTask : HazelcastFixedRateTask<MoveToEventStorageTaskDependencies> {
    companion object {
        private val RS_BATCH_SIZE =
            (ChroniclePostgresTables.MAX_BIND_PARAMETERS / RedshiftDataTables.IOS_SENSOR_DATA.columns.size)
        private const val PERIOD = 5 * 60000L
        private const val INITIAL_DELAY = 5000L
        private val UPLOAD_AT_INDEX = RedshiftDataTables.getInsertUsageEventColumnIndex(RedshiftColumns.UPLOADED_AT)

        private val logger = LoggerFactory.getLogger(RecalculateParticipantStatsTask::class.java)

        private val executor: ListeningExecutorService =
            MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(3))
    }

    override fun runTask() {
        val f = executor.submit {
            moveToEventStorage()
        }
        try {
            f.get(1, TimeUnit.HOURS)
        } catch (timeoutException: TimeoutException) {
            logger.error("Timed out after one hour when moving events to event storage.", timeoutException)
            f.cancel(true)
        } catch (ex: Exception) {
            logger.error("Exception when moving events to event storage.", ex)
        }
    }

    override fun getName(): String = Task.MOVE_IOS_DATA_TO_EVENT_STORAGE.name

    private fun moveToEventStorage() {
        with(getDependency()) {
            try {
                logger.info("Moving ios data from aurora to event storage.")
                val queueEntriesByFlavor: MutableMap<PostgresFlavor, MutableList<SensorDataRow>> = mutableMapOf()
                storageResolver.getPlatformStorage().connection.use { platform ->
                    platform.autoCommit = false
                    platform.createStatement().use { stmt ->
                        stmt.executeQuery(ChroniclePostgresTables.getMoveSql(128, UploadType.Ios)).use { rs ->
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

                                else -> throw InvalidParameterException("Invalid postgres flavor: ${postgresFlavor.name}")
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
            }
        }
    }

    private fun writeToEventStorage(
        hds: HikariDataSource,
        data: List<SensorDataRow>,
        includeOnConflict: Boolean
    ): Int {
        val studies = data.map { it.studyId.toString() }.toSet()
        val participants = data.map { it.participantId }.toSet()
        //TODO: May be based this off data being inserted instead?
        var minEventTimestamp: OffsetDateTime = OffsetDateTime.MAX
        var maxEventTimestamp: OffsetDateTime = OffsetDateTime.MIN

        return StopWatch(
            log = "writing ${data.size} entries to event storage.",
            level = Level.INFO,
            logger = logger
        ).use {
            val w = hds.connection.use { connection ->
                connection.autoCommit = false
                val insertBatchSize = min(data.size, RS_BATCH_SIZE)

                logger.info("Preparing primary insert statement (sensor data) with batch size $insertBatchSize")
                val insertSql = RedshiftDataTables.buildMultilineInsertSensorEvents(
                    insertBatchSize,
                    includeOnConflict
                )

                val pps = connection.prepareStatement(insertSql)

                val dr = data.size % RS_BATCH_SIZE

                val fps = if (data.size > RS_BATCH_SIZE && dr != 0) {
                    logger.info("Preparing secondary insert statement with batch size $dr")
                    connection.prepareStatement(
                        RedshiftDataTables.buildMultilineInsertSensorEvents(
                            dr,
                            includeOnConflict
                        )
                    )
                } else {
                    pps
                }

                val s = try {
                    data.chunked(insertBatchSize).forEach { sensorDataRows ->
                        var offset = 0
                        val ps = if (insertBatchSize == sensorDataRows.size) {
                            pps
                        } else {
                            fps
                        }

                        logger.info("Writing row of size ${sensorDataRows.size} to ios event storage.")

                        sensorDataRows.forEach {
                            val studyId = it.studyId
                            val participantId = it.participantId
                            val sourceDeviceId = it.sourceDeviceId

                            logger.trace(
                                "Writing row to storage (ios) " + ChronicleServerUtil.STUDY_PARTICIPANT_DATASOURCE,
                                studyId,
                                participantId,
                                sourceDeviceId
                            )

                            val (minOdt, maxOdt) = writeSensorDataToRedshift(ps, offset, studyId, participantId, it.sensorType, it.row)
                            minEventTimestamp = minOf(minOdt, minEventTimestamp)
                            maxEventTimestamp = maxOf(maxOdt, maxEventTimestamp)
                            offset += RedshiftDataTables.IOS_SENSOR_DATA.columns.size
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

                /*
                 * We need to remove any duplicates that were inserted. The general approach is to use min/max recorded date
                 * and (study_id, participant_id) to count duplicates within that window and remove them. The reason for using
                 * this approach is that we don't know when duplicate may be uploaded so we need a bounded way to maintain
                 * the uniqueness invariant on each upload.
                 */

                val tempTableName = "duplicate_ios_events_${RandomStringUtils.randomAlphanumeric(10)}"

                //Create a table that contains any duplicate values introduced by this latest upload for the minimum upload_at value
                StopWatch(
                    log = "Creating duplicates table for ios studies = {} and participants = {} ",
                    level = Level.INFO,
                    logger = logger,
                    studies,
                    participants
                ).use {
                    connection.createStatement()
                        .use { stmt -> stmt.execute(RedshiftDataTables.createTempTableOfDuplicates(tempTableName, IOS_SENSOR_DATA)) }
                    connection.prepareStatement(RedshiftDataTables.buildTempTableOfDuplicatesForIos(tempTableName))
                        .use { ps ->
                            ps.setArray(1, PostgresArrays.createTextArray(connection, studies))
                            ps.setArray(2, PostgresArrays.createTextArray(connection, participants))
                            ps.setObject(3, minEventTimestamp)
                            ps.setObject(4, maxEventTimestamp)
                            ps.execute()
                        }
                }

                //Delete the duplicates, if any from chronicle_usage_events and drop the temporary table.
                StopWatch(
                    log = "Deleting duplicates for ios studies = {} and participants = {} ",
                    level = Level.INFO,
                    logger = logger,
                    studies,
                    participants
                ).use {
                    connection.createStatement().use { stmt ->
                        stmt.execute(RedshiftDataTables.getDeleteIosSensorDataFromTempTable(tempTableName))
                        stmt.execute("INSERT INTO ${IOS_SENSOR_DATA.name} SELECT * FROM $tempTableName")
                        stmt.execute("DROP TABLE $tempTableName")
                    }
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
                    mapOf(it.sensorType to listOf(it.row)),
                    getDependency().studyService
                )
            }
            w
        }
    }

    /**
     * @return A pair of [OffsetDateTime] where the first element in the pair is the minimum offset datetime in the
     * data and the second element is maximum element in the data.
     */
    private fun writeSensorDataToRedshift(
        ps: PreparedStatement,
        offset: Int,
        studyId: UUID,
        participantId: String,
        sensorType: SensorType,
        dataColumns: List<SensorDataColumn>,
    ): Pair<OffsetDateTime, OffsetDateTime> {
        var minEventTimestamp: OffsetDateTime = OffsetDateTime.MAX
        var maxEventTimestamp: OffsetDateTime = OffsetDateTime.MIN
        ps.setString(
            offset + RedshiftDataTables.getInsertSensorDataColumnIndex(RedshiftColumns.STUDY_ID),
            studyId.toString()
        )
        ps.setString(
            offset + RedshiftDataTables.getInsertSensorDataColumnIndex(RedshiftColumns.PARTICIPANT_ID),
            participantId
        )
        ps.setString(
            offset + RedshiftDataTables.getInsertSensorDataColumnIndex(RedshiftColumns.SENSOR_TYPE),
            sensorType.name
        )

        dataColumns.forEach { dataColumn ->
            val col = dataColumn.col
            val index = offset + dataColumn.colIndex
            val value = dataColumn.value

            if (value!=null && col.name == RedshiftColumns.RECORDED_DATE_TIME.name) {
                val odt = odtFromUsageEventColumn(value)!!
                if (odt.isBefore(minEventTimestamp)) {
                    minEventTimestamp = odt
                }
                if (odt.isAfter(maxEventTimestamp)) {
                    maxEventTimestamp = odt
                }
            }

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
        return minEventTimestamp to maxEventTimestamp
    }

    private fun getZonedDateTime(sensorDataColumns: List<SensorDataColumn>): ZonedDateTime {
        var timezone: String? = null
        var odt: OffsetDateTime? = null
        sensorDataColumns.forEach {
            if (odt == null && it.col == RedshiftColumns.RECORDED_DATE_TIME) {
                odt = it.value as OffsetDateTime
            } else if (timezone == null && it.col == RedshiftColumns.TIMEZONE) {
                timezone = it.value as String
            }
        }
        checkNotNull(odt) { "Recorded date was null while processing upload." }
        checkNotNull(timezone) { "Timezone was null while processing upload." }
        return odt!!.atZoneSameInstant(ZoneId.of(timezone))
    }

    private fun updateParticipantStats(
        studyId: UUID,
        participantId: String,
        data: Map<SensorType, List<List<SensorDataColumn>>>,
        studyService: StudyManager
    ) {
        //TODO: We should be able to use odt directly instead of decoding with timezone as timestamp from iphone
        //should include timezone and it is preferred in upload buffer json
        val dates = data
            .values.asSequence()
            .flatMap { sensorRowsOfType -> sensorRowsOfType.map { getZonedDateTime(it) } }
            .toSet()


        val uniqueDates: Set<LocalDate> = dates.map { it.toLocalDate() }.toSet()

        val minDate = dates.min()
        val maxDate = dates.max()

        val statsUpdate = ParticipantStats(
            studyId = studyId,
            participantId = participantId,
            iosUniqueDates = uniqueDates,
            iosLastPing = OffsetDateTime.now(),
            iosFirstDate = minDate.toOffsetDateTime(),
            iosLastDate = maxDate.toOffsetDateTime(),
        )
        studyService.insertOrUpdateParticipantStats(statsUpdate)
    }

    override fun getInitialDelay(): Long = INITIAL_DELAY

    override fun getPeriod(): Long = PERIOD

    override fun getTimeUnit(): TimeUnit = TimeUnit.MILLISECONDS

    override fun getDependenciesClass(): Class<out MoveToEventStorageTaskDependencies> =
        MoveToEventStorageTaskDependencies::class.java


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
        nullifyCols(RedshiftColumns.DEVICE_USAGE_SENSOR_COLS + RedshiftColumns.MESSAGES_USAGE_SENSOR_COLS + RedshiftColumns.KEYBOARD_METRICS_SENSOR_COLS - RedshiftColumns.TOTAL_UNIQUE_CONTACTS)

    data.forEach {
        val phoneUsageData: PhoneUsageData = SensorDataUploadService.mapper.readValue(it.data)
        val cols = mutableListOf(
            SensorDataColumn(RedshiftColumns.TOTAL_INCOMING_CALLS, phoneUsageData.totalIncomingCalls),
            SensorDataColumn(RedshiftColumns.TOTAL_OUTGOING_CALLS, phoneUsageData.totalOutgoingCalls),
            SensorDataColumn(RedshiftColumns.TOTAL_CALL_DURATION, phoneUsageData.totalPhoneDuration),
            SensorDataColumn(RedshiftColumns.TOTAL_UNIQUE_CONTACTS, phoneUsageData.totalUniqueContacts)
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
        nullifyCols(RedshiftColumns.PHONE_USAGE_SENSOR_COLS + RedshiftColumns.MESSAGES_USAGE_SENSOR_COLS + RedshiftColumns.KEYBOARD_METRICS_SENSOR_COLS)

    data.forEach sample@{ sample ->
        val deviceUsageData: DeviceUsageData = SensorDataUploadService.mapper.readValue(sample.data)
        val appCategories: Set<String> = deviceUsageData.appUsage.keys + deviceUsageData.webUsage.keys
        val summaryCols = listOf(
            SensorDataColumn(RedshiftColumns.TOTAL_UNLOCK_DURATION, deviceUsageData.totalUnlockDuration),
            SensorDataColumn(RedshiftColumns.TOTAL_SCREEN_WAKES, deviceUsageData.totalScreenWakes),
            SensorDataColumn(RedshiftColumns.TOTAL_UNLOCKS, deviceUsageData.totalUnlocks)
        )

        if (appCategories.isEmpty()) {
            val cols = nullifyCols(
                setOf(
                    RedshiftColumns.APP_CATEGORY,
                    RedshiftColumns.APP_USAGE_TIME,
                    RedshiftColumns.TEXT_INPUT_DURATION,
                    RedshiftColumns.TEXT_INPUT_SOURCE,
                    RedshiftColumns.BUNDLE_IDENTIFIER,
                    RedshiftColumns.APP_CATEGORY_WEB_DURATION
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
                        RedshiftColumns.TEXT_INPUT_SOURCE,
                        RedshiftColumns.TEXT_INPUT_DURATION,
                        RedshiftColumns.APP_USAGE_TIME,
                        RedshiftColumns.APP_CATEGORY,
                        RedshiftColumns.BUNDLE_IDENTIFIER
                    )
                ).toMutableList()
                cols.add(SensorDataColumn(RedshiftColumns.APP_CATEGORY_WEB_DURATION, webUsage))
                cols.addAll(summaryCols)
                cols.addAll(defaultNullCols)
                cols.addAll(mapSharedColumns(sample))
                result.add(cols)

                return@categories
            }

            appUsages.forEach usage@{ usage ->
                if (usage.textInputSessions.isEmpty()) {
                    val cols = mutableListOf(
                        SensorDataColumn(RedshiftColumns.TEXT_INPUT_SOURCE, null),
                        SensorDataColumn(RedshiftColumns.TEXT_INPUT_DURATION, null),
                        SensorDataColumn(RedshiftColumns.APP_USAGE_TIME, usage.usageTime),
                        SensorDataColumn(RedshiftColumns.APP_CATEGORY, category),
                        SensorDataColumn(RedshiftColumns.BUNDLE_IDENTIFIER, usage.bundleIdentifier),
                        SensorDataColumn(RedshiftColumns.APP_CATEGORY_WEB_DURATION, webUsage),
                    )
                    cols.addAll(summaryCols)
                    cols.addAll(defaultNullCols)
                    cols.addAll(mapSharedColumns(sample))
                    result.add(cols)

                    return@usage
                }

                usage.textInputSessions.forEach { (inputSource, duration) ->
                    val cols = mutableListOf(
                        SensorDataColumn(RedshiftColumns.TEXT_INPUT_SOURCE, inputSource),
                        SensorDataColumn(RedshiftColumns.TEXT_INPUT_DURATION, duration),
                        SensorDataColumn(RedshiftColumns.APP_USAGE_TIME, usage.usageTime),
                        SensorDataColumn(RedshiftColumns.APP_CATEGORY, category),
                        SensorDataColumn(RedshiftColumns.BUNDLE_IDENTIFIER, usage.bundleIdentifier),
                        SensorDataColumn(RedshiftColumns.APP_CATEGORY_WEB_DURATION, webUsage)
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
    val nullCols =
        nullifyCols(RedshiftColumns.PHONE_USAGE_SENSOR_COLS + RedshiftColumns.MESSAGES_USAGE_SENSOR_COLS + RedshiftColumns.DEVICE_USAGE_SENSOR_COLS)

    data.forEach { sample ->
        val keyboardMetricsData: KeyboardMetricsData = SensorDataUploadService.mapper.readValue(sample.data)
        val sentiments =
            keyboardMetricsData.emojiCountBySentiment.keys + keyboardMetricsData.wordCountBySentiment.keys

        if (sentiments.isEmpty()) {
            val cols = mapDefaultKeyboardMetricsCols(keyboardMetricsData).toMutableList()
            cols.add(SensorDataColumn(RedshiftColumns.SENTIMENT, null))
            cols.add(SensorDataColumn(RedshiftColumns.SENTIMENT_WORD_COUNT, null))
            cols.add(SensorDataColumn(RedshiftColumns.SENTIMENT_EMOJI_COUNT, null))
            cols.addAll(mapSharedColumns(sample))
            cols.addAll(nullCols)
            result.add(cols)
            return@forEach
        }
        sentiments.forEach { sentiment ->
            val cols = mapDefaultKeyboardMetricsCols(keyboardMetricsData).toMutableList()
            cols.add(SensorDataColumn(RedshiftColumns.SENTIMENT, sentiment))
            cols.add(
                SensorDataColumn(
                    RedshiftColumns.SENTIMENT_WORD_COUNT,
                    keyboardMetricsData.wordCountBySentiment[sentiment]
                )
            )
            cols.add(
                SensorDataColumn(
                    RedshiftColumns.SENTIMENT_EMOJI_COUNT,
                    keyboardMetricsData.emojiCountBySentiment[sentiment]
                )
            )
            cols.addAll(mapSharedColumns(sample))
            cols.addAll(nullCols)
            result.add(cols)
        }
    }

    return result
}

private fun mapDefaultKeyboardMetricsCols(data: KeyboardMetricsData): List<SensorDataColumn> {
    return listOf(
        SensorDataColumn(RedshiftColumns.TOTAL_WORDS, data.totalWords),
        SensorDataColumn(RedshiftColumns.TOTAL_ALTERED_WORDS, data.totalAlteredWords),
        SensorDataColumn(RedshiftColumns.TOTAL_TAPS, data.totalTaps),
        SensorDataColumn(RedshiftColumns.TOTAL_DRAGS, data.totalDrags),
        SensorDataColumn(RedshiftColumns.TOTAL_DELETES, data.totalDeletes),
        SensorDataColumn(RedshiftColumns.TOTAL_EMOJIS, data.totalEmojis),
        SensorDataColumn(RedshiftColumns.TOTAL_PATHS, data.totalPaths),
        SensorDataColumn(RedshiftColumns.TOTAL_PATH_LENGTH, data.totalPathLength),
        SensorDataColumn(RedshiftColumns.TOTAL_PATH_TIME, data.totalPathTime),
        SensorDataColumn(RedshiftColumns.TOTAL_AUTO_CORRECTIONS, data.totalAutoCorrections),
        SensorDataColumn(RedshiftColumns.TOTAL_SPACE_CORRECTIONS, data.totalSpaceCorrections),
        SensorDataColumn(RedshiftColumns.TOTAL_TRANSPOSITION_CORRECTIONS, data.totalTranspositionCorrections),
        SensorDataColumn(RedshiftColumns.TOTAL_INSERT_KEY_CORRECTIONS, data.totalInsertKeyCorrections),
        SensorDataColumn(RedshiftColumns.TOTAL_RETRO_CORRECTIONS, data.totalRetroCorrections),
        SensorDataColumn(RedshiftColumns.TOTAL_SKIP_TOUCH_CORRECTIONS, data.totalSkipTouchCorrections),
        SensorDataColumn(RedshiftColumns.TOTAL_NEAR_KEY_CORRECTIONS, data.totalNearKeyCorrections),
        SensorDataColumn(RedshiftColumns.TOTAL_SUBSTITUTION_CORRECTIONS, data.totalSubstitutionCorrections),
        SensorDataColumn(RedshiftColumns.TOTAL_TEST_HIT_CORRECTIONS, data.totalHitTestCorrections),
        SensorDataColumn(RedshiftColumns.TOTAL_TYPING_DURATION, data.totalTypingDuration),
        SensorDataColumn(RedshiftColumns.TOTAL_PATH_PAUSES, data.totalPathPauses),
        SensorDataColumn(RedshiftColumns.TOTAL_PAUSES, data.totalPauses),
        SensorDataColumn(RedshiftColumns.TOTAL_TYPING_EPISODES, data.totalTypingEpisodes),
        SensorDataColumn(RedshiftColumns.TYPING_SPEED, data.typingSpeed),
        SensorDataColumn(RedshiftColumns.PATH_TYPING_SPEED, data.pathTypingSpeed)
    )
}

private fun mapMessagesUsageData(data: List<SensorDataSample>): List<List<SensorDataColumn>> {
    val result: MutableList<List<SensorDataColumn>> = mutableListOf()
    val nulCols =
        nullifyCols(RedshiftColumns.DEVICE_USAGE_SENSOR_COLS + RedshiftColumns.PHONE_USAGE_SENSOR_COLS + RedshiftColumns.KEYBOARD_METRICS_SENSOR_COLS - RedshiftColumns.TOTAL_UNIQUE_CONTACTS)

    data.forEach {
        val messagesUsageData: MessagesUsageData = SensorDataUploadService.mapper.readValue(it.data)
        val cols = mutableListOf(
            SensorDataColumn(RedshiftColumns.TOTAL_INCOMING_MESSAGES, messagesUsageData.totalIncomingMessages),
            SensorDataColumn(RedshiftColumns.TOTAL_OUTGOING_MESSAGES, messagesUsageData.totalOutgoingMessages),
            SensorDataColumn(RedshiftColumns.TOTAL_UNIQUE_CONTACTS, messagesUsageData.totalUniqueContacts)
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
        SensorDataColumn(RedshiftColumns.SAMPLE_ID, dataSample.id.toString()),
        SensorDataColumn(RedshiftColumns.SENSOR_TYPE, dataSample.sensor.name),
        SensorDataColumn(RedshiftColumns.SAMPLE_DURATION, dataSample.duration),
        SensorDataColumn(RedshiftColumns.RECORDED_DATE_TIME, dataSample.dateRecorded),
        SensorDataColumn(RedshiftColumns.START_DATE_TIME, dataSample.startDate),
        SensorDataColumn(RedshiftColumns.END_DATE_TIME, dataSample.endDate),
        SensorDataColumn(RedshiftColumns.TIMEZONE, dataSample.timezone),
        SensorDataColumn(RedshiftColumns.DEVICE_VERSION, device.systemVersion),
        SensorDataColumn(RedshiftColumns.DEVICE_NAME, device.name),
        SensorDataColumn(RedshiftColumns.DEVICE_MODEL, device.model),
        SensorDataColumn(RedshiftColumns.DEVICE_SYSTEM_NAME, device.name),
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
}