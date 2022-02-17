package com.openlattice.chronicle.deletion

import com.openlattice.chronicle.jobs.ChronicleStudyJobDefinition
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.CHRONICLE_USAGE_EVENTS
import java.util.*

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
class DeleteStudyUsageData(
    override val studyId: UUID
) : ChronicleStudyJobDefinition {
    var table: String = CHRONICLE_USAGE_EVENTS.name
}