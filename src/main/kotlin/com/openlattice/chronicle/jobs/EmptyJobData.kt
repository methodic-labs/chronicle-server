package com.openlattice.chronicle.jobs

import com.openlattice.chronicle.ids.IdConstants

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */

class EmptyJobData: ChronicleJobData {
    override var studyId = IdConstants.UNINITIALIZED.id
}