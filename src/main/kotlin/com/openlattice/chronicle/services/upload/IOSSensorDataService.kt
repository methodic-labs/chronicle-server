package com.openlattice.chronicle.services.upload

import com.geekbeast.util.StopWatch
import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.sensorkit.SensorDataSample
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.util.ChronicleServerUtil
import org.slf4j.LoggerFactory
import org.slf4j.event.Level
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class IOSSensorDataService(
        private val storageResolver: StorageResolver,
        private val enrollmentManager: EnrollmentManager
) : IOSSensorDataManager {
    private val logger = LoggerFactory.getLogger(IOSSensorDataService::class.java)

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
                val (flavor, hds) = storageResolver.resolve(studyId)

                val status = enrollmentManager.getParticipationStatus(organizationId, studyId, participantId)
                if (ParticipationStatus.NOT_ENROLLED == status) {
                    logger.warn(
                            "participant is not enrolled, ignoring upload" + ChronicleServerUtil.ORG_STUDY_PARTICIPANT_DATASOURCE,
                            organizationId,
                            studyId,
                            participantId,
                            deviceId
                    )
                    return 0
                }
                val isDeviceEnrolled = enrollmentManager.isKnownDatasource(
                        organizationId, studyId, participantId, deviceId
                )
                if (isDeviceEnrolled) {
                    logger.error(
                            "data source not found, ignoring upload" + ChronicleServerUtil.ORG_STUDY_PARTICIPANT_DATASOURCE,
                            organizationId,
                            studyId,
                            participantId,
                            deviceId
                    )
                    return 0
                }

                logger.info(
                        "attempting to log data" + ChronicleServerUtil.ORG_STUDY_PARTICIPANT_DATASOURCE,
                        organizationId,
                        studyId,
                        participantId,
                        deviceId
                )
                val mappedData = mapToStorageModel(data)
                return 0 // need to return number of rows written
            } catch (ex: Exception) {
                return 0
            }
        }
    }

    private fun mapToStorageModel(data: List<SensorDataSample>) {

    }
}