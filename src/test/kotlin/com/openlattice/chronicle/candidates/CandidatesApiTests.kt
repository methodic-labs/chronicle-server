package com.openlattice.chronicle.candidates

import com.openlattice.chronicle.ChronicleServerTests
import com.openlattice.chronicle.ids.IdConstants
import org.junit.Assert
import org.junit.Test
import java.time.LocalDate

class CandidatesApiTests : ChronicleServerTests() {

    @Test
    fun testRegisterCandidate() {
        val expected = Candidate(firstName = "iron", lastName = "man", dob = LocalDate.parse("2008-05-02"))
        val candidateId = clientUser1.candidatesApi.registerCandidate(expected)
        Assert.assertNotEquals(IdConstants.UNINITIALIZED.id, candidateId)
        expected.candidateId = candidateId
        val actual = clientUser1.candidatesApi.getCandidate(candidateId)
        Assert.assertEquals(expected, actual)
    }
}
