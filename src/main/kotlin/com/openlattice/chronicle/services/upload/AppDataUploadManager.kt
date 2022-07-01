package com.openlattice.chronicle.services.upload

import com.google.common.collect.SetMultimap
import com.openlattice.chronicle.android.ChronicleUsageEvent
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
interface AppDataUploadManager {
    fun upload(
            studyId: UUID,
            participantId: String,
            sourceDeviceId: String,
            data: List<SetMultimap<UUID, Any>>
    ): Int

    fun uploadAndroidUsageEvents(
        studyId: UUID,
        participantId: String,
        sourceDeviceId: String,
        data: List<ChronicleUsageEvent>
    ): Int
}