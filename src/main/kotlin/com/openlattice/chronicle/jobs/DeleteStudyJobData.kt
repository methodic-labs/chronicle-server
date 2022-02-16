package com.openlattice.chronicle.jobs

import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.CHRONICLE_USAGE_EVENTS
import java.util.*

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
class DeleteStudyUsageData(
    override var studyId: UUID
): ChronicleJobData {
    var table: String = CHRONICLE_USAGE_EVENTS.name
}