package com.openlattice.chronicle.services.candidates

import com.openlattice.chronicle.candidates.Candidate
import java.sql.Connection
import java.util.UUID

interface CandidatesManager {
    fun getCandidate(candidateId: UUID): Candidate
    fun getCandidates(candidateIds: Set<UUID>): Iterable<Candidate>
    fun registerCandidate(connection: Connection, candidate: Candidate)
}
