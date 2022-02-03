package com.openlattice.chronicle.services.upload

import com.geekbeast.util.StopWatch
import com.geekbeast.util.log
import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.sensorkit.SensorDataSample
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.IOS_SENSOR
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.util.ChronicleServerUtil
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.slf4j.event.Level
import java.time.OffsetDateTime
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class IOSSensorDataService(
        private val storageResolver: StorageResolver,
) : IOSSensorDataManager {

    companion object {
        private val logger = LoggerFactory.getLogger(IOSSensorDataService::class.java)

        private val COLUMNS = IOS_SENSOR.columns.joinToString(",") { it.name }

        /**
         * PreparedStatement bind order
         * 1) organizationId,
         * 2) studyId,
         * 3) participantId,
         * 4) id
         * 5) sensorType
         * 6) dateRecorded
         * 7) duration
         * 8) timezone
         * 9) sensorData,
         * 10) device
         */
        private val INSERT_SENSOR_DATA_SQL = """
            INSERT INTO ${IOS_SENSOR.name}($COLUMNS) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
            ON CONFLICT DO NOTHING
        """.trimIndent()
    }
    override fun uploadData(
            organizationId: UUID,
            studyId: UUID,
            participantId: String,
            deviceId: String,
            data: List<SensorDataSample>
    ): Int {
        StopWatch(
                log = "logging ${data.size} entries for ${ChronicleServerUtil.ORG_STUDY_PARTICIPANT_DATASOURCE}",
                level = Level.INFO,
                logger = logger,
                data.size,
                organizationId,
                studyId,
                participantId,
                deviceId
        ).use {
            try {

                logger.info(
                        "attempting to log data" + ChronicleServerUtil.ORG_STUDY_PARTICIPANT_DATASOURCE,
                        organizationId,
                        studyId,
                        participantId,
                        deviceId
                )
                val written = writeDataToStorage(organizationId, studyId, participantId, deviceId, data)
                if (written != data.size) {
                    logger.warn("Wrote $written entities, but wrote ${data.size}")
                }

                return written
            } catch (ex: Exception) {
                return 0
            }
        }
    }

    private fun writeDataToStorage(
            organizationId: UUID,
            studyId: UUID,
            participantId: String,
            deviceId: String,
            data: List<SensorDataSample>
    ) : Int {

        val (_, hds) = storageResolver.resolveAndGetFlavor(studyId)

        return hds.connection.use { connection ->
            try {
                val wc = connection.prepareStatement(INSERT_SENSOR_DATA_SQL).use { ps ->
                    ps.setString(1, organizationId.toString())
                    ps.setString(2, studyId.toString())
                    ps.setString(3, participantId)

                    data.forEach { sample ->
                        ps.setString(4, sample.id.toString())
                        ps.setString(5, sample.sensor)
                        ps.setObject(6, sample.dateRecorded)
                        ps.setDouble(7, sample.duration)
                        ps.setString(8, sample.timezone)
                        ps.setString(9, sample.data)
                        ps.setString(10, sample.device)

                        ps.addBatch()
                    }
                    ps.executeBatch().sum()
                }
                return@use wc
            } catch (ex: Exception) {
                logger.error(
                        "error writing sensor data" + ChronicleServerUtil.ORG_STUDY_PARTICIPANT_DATASOURCE,
                        organizationId,
                        studyId,
                        deviceId
                )
                return 0
            }
        }
    }
}