package com.openlattice.chronicle.candidates

import com.openlattice.chronicle.ChronicleServerTests
import com.openlattice.chronicle.client.ChronicleClient
import com.openlattice.chronicle.ids.IdConstants
import org.junit.Test
import java.lang.IllegalArgumentException

class CandidatesApiTests : ChronicleServerTests() {

    private val chronicleClient: ChronicleClient = clientUser1

    @Test(expected = IllegalArgumentException::class)
    fun testBadCandidateDob1() {
        chronicleClient.candidatesApi.registerCandidate(badCandidateDob("03-12-1989"))
    }

    @Test(expected = IllegalArgumentException::class)
    fun testBadCandidateDob2() {
        chronicleClient.candidatesApi.registerCandidate(badCandidateDob("03/12/1989"))
    }

    @Test(expected = IllegalArgumentException::class)
    fun testBadCandidateDob3() {
        chronicleClient.candidatesApi.registerCandidate(badCandidateDob("12-03-1989"))
    }

    @Test(expected = IllegalArgumentException::class)
    fun testBadCandidateDob4() {
        chronicleClient.candidatesApi.registerCandidate(badCandidateDob("12/03/1989"))
    }

    private fun badCandidateDob(dob: String): Candidate {
        return Candidate(IdConstants.UNINITIALIZED.id, null, null, null, dob)
    }
}
