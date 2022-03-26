package com.openlattice.chronicle.deletion

import com.openlattice.chronicle.services.jobs.ChronicleParticipantJobDefinition
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.TIME_USE_DIARY_SUBMISSIONS
import java.util.UUID

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
class DeleteParticipantTUDSubmissionData (
    override val studyId: UUID,
    override val participantIds: Collection<String>
) : ChronicleParticipantJobDefinition {
    companion object {
        val table: String = TIME_USE_DIARY_SUBMISSIONS.name
    }
}