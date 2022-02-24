package com.openlattice.chronicle.deletion

import com.openlattice.chronicle.jobs.ChronicleParticipantJobDefinition
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.CHRONICLE_USAGE_EVENTS
import java.util.UUID

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
class DeleteParticipantUsageData (
    override val studyId: UUID,
    override val participantId: UUID
) : ChronicleParticipantJobDefinition {
    companion object {
        val table: String = CHRONICLE_USAGE_EVENTS.name
    }
}