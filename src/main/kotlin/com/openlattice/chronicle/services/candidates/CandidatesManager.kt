package com.openlattice.chronicle.services.candidates

import com.openlattice.chronicle.candidates.Candidate
import java.sql.Connection

interface CandidatesManager {
    fun registerCandidate(connection: Connection, candidate: Candidate)
}
