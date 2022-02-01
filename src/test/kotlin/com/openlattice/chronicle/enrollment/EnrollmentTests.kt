package com.openlattice.chronicle.enrollment

import com.google.common.base.Optional
import com.openlattice.chronicle.ChronicleServerTests
import com.openlattice.chronicle.client.ChronicleClient
import com.openlattice.chronicle.sources.AndroidDevice
import com.openlattice.chronicle.study.Study
import org.junit.Test
import org.slf4j.LoggerFactory
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class EnrollmentTests : ChronicleServerTests() {
    private val chronicleClient: ChronicleClient = clientUser1


    @Test
    fun testEnrollment() {
        val studyApi = chronicleClient.studyApi
        val organizationId = UUID.randomUUID()
        val participantId = "test1234"
        val sourceDeviceId = "device1234"
        val sourceDevice = AndroidDevice(
            "Samsung",
            "P",
            "Chocholate Chip",
            "Samsung",
            "21",
            "21",
            "",
            "",
            Optional.of(mutableMapOf())
        )

        val expected = Study(title = "This is a test study.", contact = "tests@openlattice.com")
        val studyId = studyApi.createStudy(expected)
        LoggerFactory.getLogger(EnrollmentTests::class.java).info(" Serialization: ")
        val deviceId = studyApi.enroll(studyId, participantId, sourceDeviceId, sourceDevice)
    }
}