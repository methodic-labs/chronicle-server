package com.openlattice.chronicle.services.studies

import com.openlattice.chronicle.study.Study
import java.sql.Connection
import java.util.UUID

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
interface StudyManager {
    fun createStudy(connection: Connection, study: Study)
    fun getStudy(studyIds: Collection<UUID>): Iterable<Study>
    fun updateStudy(connection: Connection, studyId: UUID, study: Study)
}