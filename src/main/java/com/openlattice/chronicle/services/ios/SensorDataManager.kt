package com.openlattice.chronicle.services.ios

import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
interface SensorDataManager {
    fun uploadData( organizationId: UUID,
                    studyId: UUID,
                    participantId: String,
                    deviceId: String,
                    data: List<Map<UUID, Any>>)
}