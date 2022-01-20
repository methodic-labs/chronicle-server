package com.openlattice.chronicle.study

import com.openlattice.chronicle.ChronicleServerTests
import com.openlattice.chronicle.client.ChronicleClient
import org.junit.Test

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class StudyTests : ChronicleServerTests() {
    private val chronicleClient: ChronicleClient = clientUser1


    @Test
    fun createStudy() {
        val studyApi = chronicleClient.studyApi
        val expected = Study(title = "This is a test study.")
        val studyId = studyApi.createStudy(expected)
        val actual = studyApi.getStudy(studyId)
    }
}