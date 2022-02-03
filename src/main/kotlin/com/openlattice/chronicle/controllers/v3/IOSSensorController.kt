package com.openlattice.chronicle.controllers.v3

import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.sensorkit.IOSSensorApi
import com.openlattice.chronicle.sensorkit.IOSSensorApi.Companion.CONTROLLER
import com.openlattice.chronicle.sensorkit.IOSSensorApi.Companion.DATASOURCE_ID
import com.openlattice.chronicle.sensorkit.IOSSensorApi.Companion.DATASOURCE_ID_PATH
import com.openlattice.chronicle.sensorkit.IOSSensorApi.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.sensorkit.IOSSensorApi.Companion.ORGANIZATION_ID_PATH
import com.openlattice.chronicle.sensorkit.IOSSensorApi.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.sensorkit.IOSSensorApi.Companion.PARTICIPANT_ID_PATH
import com.openlattice.chronicle.sensorkit.IOSSensorApi.Companion.STUDY_ID
import com.openlattice.chronicle.sensorkit.IOSSensorApi.Companion.STUDY_ID_PATH
import com.openlattice.chronicle.sensorkit.SensorDataSample
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.services.upload.IOSSensorDataManager
import com.openlattice.chronicle.services.upload.IOSSensorDataService
import com.openlattice.chronicle.util.ChronicleServerUtil
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import retrofit2.http.POST
import java.util.*
import javax.inject.Inject

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */

@RestController
@RequestMapping(CONTROLLER)
class IOSSensorController: IOSSensorApi {
    @Inject
    private lateinit var iosSensorDataService: IOSSensorDataService

    @Inject
    private lateinit var enrollmentManager: EnrollmentManager

    companion object {
        private val logger = LoggerFactory.getLogger(IOSSensorController::class.java)
    }

    @PostMapping(
            path = [ORGANIZATION_ID_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH + DATASOURCE_ID_PATH],
            consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun uploadIOSSensorData(
            @PathVariable(ORGANIZATION_ID) organizationId: UUID,
            @PathVariable (STUDY_ID) studyId: UUID,
            @PathVariable (PARTICIPANT_ID) participantId: String,
            @PathVariable(DATASOURCE_ID)  deviceId: String,
            @RequestBody data: List<SensorDataSample>): Int {

        val status = enrollmentManager.getParticipationStatus( studyId, participantId )
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
        val isDeviceEnrolled = enrollmentManager.isKnownDatasource( studyId, participantId, deviceId )
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
        return iosSensorDataService.uploadData(organizationId, studyId, participantId, deviceId, data)
    }
}