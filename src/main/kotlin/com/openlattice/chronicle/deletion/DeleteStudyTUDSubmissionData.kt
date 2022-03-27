package com.openlattice.chronicle.deletion

import com.openlattice.chronicle.services.jobs.ChronicleStudyJobDefinition
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.TIME_USE_DIARY_SUBMISSIONS
import java.util.UUID

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
class DeleteStudyTUDSubmissionData (
    override val studyId: UUID
) : ChronicleStudyJobDefinition {
    var table: String = TIME_USE_DIARY_SUBMISSIONS.name
}