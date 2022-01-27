package com.openlattice.chronicle.services.candidates

import com.openlattice.chronicle.candidates.Candidate
import java.sql.Connection

interface CandidatesManager {
    fun createCandidate(connection: Connection, candidate: Candidate)
}
