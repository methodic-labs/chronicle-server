package com.openlattice.chronicle.services.download

import com.openlattice.chronicle.constants.ParticipantDataType
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
interface DataDownloadManager {

    fun getParticipantData(
            organizationId: UUID?,
            studyId: UUID,
            participantEntityId: UUID,
            dataType: ParticipantDataType,
            token: String?): Iterable<Map<String, Set<Any>>>
}