package com.openlattice.chronicle.deletion

import com.openlattice.chronicle.jobs.ChronicleStudyJobDefinition
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.APP_USAGE_SURVEY
import java.util.UUID

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
class DeleteStudyAppUsageSurveyData (
    override val studyId: UUID
) : ChronicleStudyJobDefinition {
    var table: String = APP_USAGE_SURVEY.name
}