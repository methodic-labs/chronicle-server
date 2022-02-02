package com.openlattice.chronicle.enrollment

import com.auth0.json.mgmt.client.IOS
import com.geekbeast.retrofit.RhizomeRetrofitCallFailedException
import com.google.common.base.Optional
import com.openlattice.chronicle.ChronicleServerTests
import com.openlattice.chronicle.client.ChronicleClient
import com.openlattice.chronicle.sources.AndroidDevice
import com.openlattice.chronicle.sources.IOSDevice
import com.openlattice.chronicle.study.Study
import com.openlattice.chronicle.util.TestDataFactory
import org.apache.commons.lang3.RandomStringUtils
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
        val participant = TestDataFactory.participant()
        val participantId = participant.participantId

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
        val candidateId = studyApi.registerParticipant(studyId, participant)
        LoggerFactory.getLogger(EnrollmentTests::class.java).info(" Serialization: ")
        val deviceId = studyApi.enroll(studyId, participantId, sourceDeviceId, sourceDevice)
    }

    @Test
    fun testIOSEnrollment() {
        val studyApi = chronicleClient.studyApi
        val participant = TestDataFactory.participant()
        val participantId = participant.participantId

        val sourceDeviceId = "iosTestDevice"
        val sourceDevice = IOSDevice(
                "iPhone 13 Pro Max",
                "iOS",
                "iPhone",
                "iPhone",
                "15.2",
                "61DB1549-A9E1-4C90-B567-D8657EDF19CB"
        )
        val study = Study(title = "Test Study", contact = "tests@openlattice.com")
        val studyId = studyApi.createStudy(study)
        studyApi.registerParticipant(studyId, participant)
        val deviceId = studyApi.enroll(studyId, participantId, sourceDeviceId, sourceDevice)

    }

    @Test(expected=RhizomeRetrofitCallFailedException::class)
    fun testEnrollmentWithBadParticipantId() {
        val studyApi = chronicleClient.studyApi
        val participant = TestDataFactory.participant()
        val participantId = RandomStringUtils.randomAlphabetic(16)

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
        val candidateId = studyApi.registerParticipant(studyId, participant)
        LoggerFactory.getLogger(EnrollmentTests::class.java).info(" Serialization: ")
        val deviceId = studyApi.enroll(studyId, participantId, sourceDeviceId, sourceDevice)
    }
}