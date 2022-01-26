package com.openlattice.chronicle.services.upload

import com.openlattice.chronicle.sensorkit.SensorDataSample
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
interface IOSSensorDataManager {
    fun uploadData(organizationId: UUID,
                   studyId: UUID,
                   participantId: String,
                   deviceId: String,
                   data: List<SensorDataSample>
    ): Int
}