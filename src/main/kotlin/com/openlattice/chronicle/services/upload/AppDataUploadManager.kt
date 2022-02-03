package com.openlattice.chronicle.services.upload

import com.google.common.collect.SetMultimap
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
interface AppDataUploadManager {
    fun upload(
        organizationId: UUID,
        studyId: UUID,
        participantId: String,
        sourceDeviceId: String,
        data: List<SetMultimap<UUID, Any>>
    ): Int

}
