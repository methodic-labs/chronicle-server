package com.openlattice.chronicle.services.studies

import com.openlattice.chronicle.study.Study
import java.util.UUID

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
interface StudyManager {
    fun createStudy(study: Study): UUID
}