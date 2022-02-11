package com.openlattice.chronicle.serialization

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.geekbeast.mappers.mappers.ObjectMappers
import com.openlattice.chronicle.sensorkit.KeyboardMetricsData
import com.openlattice.chronicle.sensorkit.PhoneUsageData
import org.junit.Test

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class SensorDataDeserializationTests {
    private val mapper: ObjectMapper = ObjectMappers.newJsonMapper()

    @Test
    fun testPhoneUsageDeserialization() {
        val str = """{
            "totalIncomingCalls": 4,
            "totalOutgoingCalls": 3,
            "totalPhoneDuration": 21,
            "totalUniqueContacts": 1
        }""".trimIndent()

        val phoneUsageData: PhoneUsageData = mapper.readValue(str)
        assert(phoneUsageData.totalIncomingCalls == 4)
        assert(phoneUsageData.totalOutgoingCalls == 3)
        assert(phoneUsageData.totalPhoneDuration == 21.0)
        assert(phoneUsageData.totalUniqueContacts == 1)
    }

    @Test
    fun testDeserializingWithNullFields() {
        val str = """{
            "totalDrags":379,
            "totalTypingDuration":0,
            "totalPaths":90,
            "totalWords":34,
            "totalPathLength":203,
            "totalAutoCorrections":9,
            "totalDeletes":392,
            "totalTranspositionCorrections":2,
            "totalEmojis":90,
            "totalNearKeyCorrections":39,
            "totalHitTestCorrections":0,
            "totalSpaceCorrections":0,
            "totalInsertKeyCorrections":20,
            "totalRetroCorrections":89,
            "emojiCountBySentiment":{"happy":77,"angry":45},
            "totalPathTime":92,
            "totalTaps":93,
            "totalSubstitutionCorrections":2,
            "totalSkipTouchCorrections":920,
            "totalAlteredWords":98,
            "wordCountBySentiment":{"excited":93,"down":89}
        }""".trimIndent()

        val keyboardMetricsData: KeyboardMetricsData = mapper.readValue(str)
        assert(keyboardMetricsData.pathTypingSpeed == null)
        assert(keyboardMetricsData.totalPathPauses == null)
        assert(keyboardMetricsData.totalTypingEpisodes == null)
        assert(keyboardMetricsData.typingSpeed == null)
        assert(keyboardMetricsData.pathTypingSpeed == null)
    }
}
