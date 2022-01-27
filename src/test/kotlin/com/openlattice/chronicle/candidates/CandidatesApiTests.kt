package com.openlattice.chronicle.candidates

import com.openlattice.chronicle.ChronicleServerTests
import com.openlattice.chronicle.client.ChronicleClient
import org.junit.Test

class CandidatesApiTests : ChronicleServerTests() {

    private val chronicleClient: ChronicleClient = clientUser1

    @Test
    fun testRegisterCandidate() {
        val c = Candidate(name = "ironman")
        chronicleClient.candidatesApi.registerCandidate(c)
    }
}
