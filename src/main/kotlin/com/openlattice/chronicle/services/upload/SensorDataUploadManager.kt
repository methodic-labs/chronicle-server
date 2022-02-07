package com.openlattice.chronicle.services.upload

import com.openlattice.chronicle.sensorkit.SensorDataSample
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
interface SensorDataUploadManager {
    fun upload(
            studyId: UUID,
            participantId: String,
            sourceDeviceId: String,
            data: List<SensorDataSample>
    ): Int
}