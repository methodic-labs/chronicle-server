package com.openlattice.chronicle.candidates

import com.openlattice.chronicle.ChronicleServerTests
import com.openlattice.chronicle.ids.IdConstants
import org.junit.Assert
import org.junit.Test
import java.time.LocalDate

class CandidatesApiTests : ChronicleServerTests() {

    @Test
    fun testGetCandidates() {
        val c1 = Candidate(firstName = "iron", lastName = "man", dateOfBirth = LocalDate.parse("2008-05-02"))
        val c1Id = clientUser1.candidatesApi.registerCandidate(c1)
        c1.id = c1Id
        val c2 = Candidate(firstName = "black", lastName = "panther", dateOfBirth = LocalDate.parse("2018-02-16"))
        val c2Id = clientUser1.candidatesApi.registerCandidate(c2)
        c2.id = c2Id
        val expected = listOf(c1, c2)
        val actual = clientUser1.candidatesApi.getCandidates(setOf(c1Id, c2Id))
        Assert.assertEquals(expected, actual)
    }

    @Test
    fun testRegisterCandidate() {
        val expected = Candidate(firstName = "iron", lastName = "man", dateOfBirth = LocalDate.parse("2008-05-02"))
        val candidateId = clientUser1.candidatesApi.registerCandidate(expected)
        Assert.assertNotEquals(IdConstants.UNINITIALIZED.id, candidateId)
        expected.id = candidateId
        val actual = clientUser1.candidatesApi.getCandidate(candidateId)
        Assert.assertEquals(expected, actual)
    }
}
