package com.openlattice.chronicle.services.candidates

import com.openlattice.chronicle.candidates.Candidate
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.CANDIDATES
import com.openlattice.chronicle.storage.PostgresColumns.Companion.EXPIRATION_DATE
import com.openlattice.chronicle.storage.StorageResolver
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.sql.Connection

@Service
class CandidatesService(
    private val storageResolver: StorageResolver,
) : CandidatesManager {

    companion object {
        private val logger = LoggerFactory.getLogger(CandidatesService::class.java)

        private val CANDIDATES_COLUMNS = CANDIDATES
            .columns
            .subtract(setOf(EXPIRATION_DATE))

        private val INSERT_CANDIDATE_SQL = """
            INSERT INTO ${CANDIDATES.name} (${CANDIDATES_COLUMNS.joinToString { it.name }}) 
            VALUES (${CANDIDATES_COLUMNS.map { "?" }.joinToString()})
        """.trimIndent()
    }

    override fun registerCandidate(connection: Connection, candidate: Candidate) {
        insertCandidate(connection, candidate)
    }

    private fun insertCandidate(connection: Connection, candidate: Candidate) {
        connection.prepareStatement(INSERT_CANDIDATE_SQL).use { ps ->
            var index = 1
            ps.setObject(index++, candidate.candidateId)
            ps.setString(index++, candidate.firstName)
            ps.setString(index++, candidate.lastName)
            ps.setString(index++, candidate.name)
            ps.setObject(index++, candidate.dateOfBirth)
            ps.setString(index++, candidate.email)
            ps.setString(index, candidate.phoneNumber)
            ps.executeUpdate()
        }
    }
}
