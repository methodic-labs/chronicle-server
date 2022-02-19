package com.openlattice.chronicle.deletion

import com.openlattice.chronicle.jobs.ChronicleParticipantJobDefinition
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.APP_USAGE_SURVEY
import java.util.UUID

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
class DeleteParticipantAppUsageSurveyData (
    override val studyId: UUID,
    override val participantIds: Collection<UUID>
) : ChronicleParticipantJobDefinition {
    companion object {
        val table: String = APP_USAGE_SURVEY.name
    }
}