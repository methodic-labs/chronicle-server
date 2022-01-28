package com.openlattice.chronicle.services.candidates

import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.SecurableObjectType
import com.openlattice.chronicle.authorization.principals.Principals
import com.openlattice.chronicle.candidates.Candidate
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.CANDIDATES
import com.openlattice.chronicle.storage.PostgresColumns.Companion.CANDIDATE_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.EXPIRATION_DATE
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.util.ensureVanilla
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.sql.Connection
import java.util.UUID

@Service
class CandidatesService(
    private val storageResolver: StorageResolver,
    private val authorizationService: AuthorizationManager,
) : CandidatesManager {

    companion object {
        private val logger = LoggerFactory.getLogger(CandidatesService::class.java)

        private val CANDIDATES_COLUMNS = CANDIDATES
            .columns
            .subtract(setOf(EXPIRATION_DATE))

        private val SELECT_CANDIDATES_SQL = """
            SELECT * FROM ${CANDIDATES.name} WHERE ${CANDIDATE_ID.name} = ANY(?)
        """.trimIndent()

        private val INSERT_CANDIDATE_SQL = """
            INSERT INTO ${CANDIDATES.name} (${CANDIDATES_COLUMNS.joinToString { it.name }}) 
            VALUES (${CANDIDATES_COLUMNS.map { "?" }.joinToString()})
        """.trimIndent()

    }

    override fun getCandidate(candidateId: UUID): Candidate {
        return selectCandidates(setOf(candidateId)).first()
    }

    override fun getCandidates(candidateIds: Set<UUID>): Iterable<Candidate> {
        return selectCandidates(candidateIds)
    }

    override fun registerCandidate(connection: Connection, candidate: Candidate) {
        insertCandidate(connection, candidate)
        authorizationService.createSecurableObject(
            connection = connection,
            aclKey = AclKey(candidate.id),
            principal = Principals.getCurrentUser(),
            objectType = SecurableObjectType.Candidate
        )
    }

    //
    //
    // private
    //
    //

    private fun insertCandidate(connection: Connection, candidate: Candidate) {
        connection.prepareStatement(INSERT_CANDIDATE_SQL).use { ps ->
            var index = 1
            ps.setObject(index++, candidate.id)
            ps.setString(index++, candidate.firstName)
            ps.setString(index++, candidate.lastName)
            ps.setString(index++, candidate.name)
            ps.setObject(index++, candidate.dateOfBirth)
            ps.setString(index++, candidate.email)
            ps.setString(index, candidate.phoneNumber)
            ps.executeUpdate()
        }
    }

    private fun selectCandidates(candidateIds: Collection<UUID>): Iterable<Candidate> {
        val (flavor, hds) = storageResolver.getPlatformStorage()
        ensureVanilla(flavor)
        return BasePostgresIterable(
            PreparedStatementHolderSupplier(hds, SELECT_CANDIDATES_SQL) { ps ->
                ps.setArray(1, PostgresArrays.createUuidArray(ps.connection, candidateIds))
            }
        ) { ResultSetAdapters.candidate(it) }
    }
}
