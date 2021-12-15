package com.openlattice.chronicle.services.delete

import com.openlattice.chronicle.data.ChronicleDeleteType
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface DataDeletionManager {
    fun deleteParticipantData(
            organizationId: UUID,
            studyId: UUID,
            participantId: String,
            chronicleDeleteType: ChronicleDeleteType,
    )

    fun deleteStudyData(
            organizationId: UUID,
            studyId: UUID,
            chronicleDeleteType: ChronicleDeleteType,
    )
}
