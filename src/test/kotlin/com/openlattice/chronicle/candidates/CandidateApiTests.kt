package com.openlattice.chronicle.candidates

import com.geekbeast.retrofit.RhizomeRetrofitCallException
import com.openlattice.chronicle.ChronicleServerTests
import com.openlattice.chronicle.ids.IdConstants
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import java.time.LocalDate
import java.util.UUID

class CandidateApiTests : ChronicleServerTests() {

    private var c1: Candidate? = null
    private var c2: Candidate? = null

    @Before
    fun beforeEachTest() {
        c1 = Candidate(firstName = "iron", lastName = "man", dateOfBirth = LocalDate.parse("2008-05-02"))
        c2 = Candidate(firstName = "black", lastName = "panther", dateOfBirth = LocalDate.parse("2018-02-16"))
    }

    @After
    fun afterEachTest() {
        c1 = null
        c2 = null
    }

    @Test
    fun testGetCandidates() {
        val id1 = clientUser1.candidateApi.registerCandidate(c1!!)
        c1!!.id = id1
        val id2 = clientUser1.candidateApi.registerCandidate(c2!!)
        c2!!.id = id2
        val expected = listOf(c1, c2)
        val actual = clientUser1.candidateApi.getCandidates(setOf(id1, id2))
        Assert.assertEquals(expected, actual)
    }

    @Test
    fun testRegisterCandidate() {
        val id = clientUser1.candidateApi.registerCandidate(c1!!)
        Assert.assertNotEquals(IdConstants.UNINITIALIZED.id, id)
        c1!!.id = id
        val actual = clientUser1.candidateApi.getCandidate(id)
        Assert.assertEquals(c1, actual)
    }

    @Test
    fun testRegisterExistingCandidate() {
        try {
            val id = clientUser1.candidateApi.registerCandidate(c1!!)
            c1!!.id = id
            clientUser1.candidateApi.registerCandidate(c1!!)
            Assert.fail()
        }
        catch (e: RhizomeRetrofitCallException) {
            Assert.assertTrue(
                "should fail with expected error message",
                e.body.contains("cannot register candidate with the given id")
            )
        }
    }

    @Test
    fun testRegisterCandidateWithRandomId() {
        try {
            c1!!.id = UUID.randomUUID()
            clientUser1.candidateApi.registerCandidate(c1!!)
            Assert.fail()
        }
        catch (e: RhizomeRetrofitCallException) {
            Assert.assertTrue(
                "should fail with expected error message",
                e.body.contains("cannot register candidate with the given id")
            )
        }
    }
}
