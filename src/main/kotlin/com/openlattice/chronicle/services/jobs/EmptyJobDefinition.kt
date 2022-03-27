package com.openlattice.chronicle.services.jobs

import com.openlattice.chronicle.ids.IdConstants

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */

class EmptyJobDefinition: ChronicleStudyJobDefinition {
    override var studyId = IdConstants.UNINITIALIZED.id
}