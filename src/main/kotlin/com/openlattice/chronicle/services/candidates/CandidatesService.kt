package com.openlattice.chronicle.services.candidates

import com.openlattice.chronicle.auditing.AuditingComponent
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.candidates.Candidate
import org.apache.commons.lang3.NotImplementedException
import java.sql.Connection

class CandidatesService(
    override val auditingManager: AuditingManager
) : CandidatesManager, AuditingComponent {

    override fun createCandidate(connection: Connection, candidate: Candidate) {
        throw NotImplementedException("coming soon")
    }
}
