package com.openlattice.chronicle.candidates

import com.openlattice.chronicle.ChronicleServerTests
import org.junit.Test

class CandidatesApiTests : ChronicleServerTests() {

    @Test
    fun testRegisterCandidate() {
        val c = Candidate(name = "ironman")
        clientUser1.candidatesApi.registerCandidate(c)
    }
}
