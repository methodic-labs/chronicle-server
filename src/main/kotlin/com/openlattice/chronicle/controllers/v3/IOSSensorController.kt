package com.openlattice.chronicle.controllers.v3

import com.openlattice.chronicle.sensorkit.IOSSensorApi
import com.openlattice.chronicle.sensorkit.IOSSensorApi.Companion.CONTROLLER
import com.openlattice.chronicle.sensorkit.IOSSensorApi.Companion.DATASOURCE_ID
import com.openlattice.chronicle.sensorkit.IOSSensorApi.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.sensorkit.IOSSensorApi.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.sensorkit.IOSSensorApi.Companion.STUDY_ID
import com.openlattice.chronicle.sensorkit.SensorDataSample
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import retrofit2.http.POST
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */

@RestController
@RequestMapping(CONTROLLER)
class IOSSensorController: IOSSensorApi {
    @PostMapping(
            path = [ IOSSensorApi.BASE + IOSSensorApi.ORGANIZATION_ID_PATH + IOSSensorApi.STUDY_ID_PATH + IOSSensorApi.PARTICIPANT_ID_PATH + IOSSensorApi.DATASOURCE_ID_PATH],
            consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun uploadIOSSensorData(
            @PathVariable(ORGANIZATION_ID) organizationId: UUID,
            @PathVariable (STUDY_ID) studyId: UUID,
            @PathVariable (PARTICIPANT_ID) participantId: String,
            @PathVariable(DATASOURCE_ID)  deviceId: String,
            @RequestBody data: List<SensorDataSample>) {
        TODO("Not yet implemented")
    }
}