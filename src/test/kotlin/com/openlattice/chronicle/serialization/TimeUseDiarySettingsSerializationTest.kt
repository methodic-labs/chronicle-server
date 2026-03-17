package com.openlattice.chronicle.serialization

import com.fasterxml.jackson.module.kotlin.readValue
import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.serializer.serializer.AbstractJacksonSerializationTest
import com.openlattice.chronicle.timeusediary.TimeUseDiarySettings
import org.junit.Assert
import org.junit.Test

class TimeUseDiarySettingsSerializationTest : AbstractJacksonSerializationTest<TimeUseDiarySettings>() {

    override fun getSampleData(): TimeUseDiarySettings {
        return TimeUseDiarySettings(
            enableChangesForSherbrookeUniversity = false,
            enableChangesForOhioStateUniversity = true,
            language = "en",
            clockFormat = 24
        )
    }

    override fun getClazz(): Class<TimeUseDiarySettings> {
        return TimeUseDiarySettings::class.java
    }

    /**
     * Verifies backwards compatibility: deserializing JSON without the clockFormat field
     * should default to 12-hour format.
     */
    @Test
    fun testDeserializeWithoutClockFormatDefaultsTo12() {
        val json = """
            {
                "@class": "com.openlattice.chronicle.timeusediary.TimeUseDiarySettings",
                "enableChangesForSherbrookeUniversity": false,
                "enableChangesForOhioStateUniversity": false,
                "language": "en"
            }
        """.trimIndent()

        val mapper = ObjectMappers.getJsonMapper()
        val settings: TimeUseDiarySettings = mapper.readValue(json)

        Assert.assertEquals(12, settings.clockFormat)
        Assert.assertEquals("en", settings.language)
        Assert.assertFalse(settings.enableChangesForSherbrookeUniversity)
        Assert.assertFalse(settings.enableChangesForOhioStateUniversity)
    }

    /**
     * Verifies that clockFormat = 24 round-trips correctly through serialization.
     */
    @Test
    fun testClockFormat24RoundTrip() {
        val mapper = ObjectMappers.getJsonMapper()
        val settings = TimeUseDiarySettings(clockFormat = 24)
        val json = mapper.writeValueAsString(settings)
        val deserialized: TimeUseDiarySettings = mapper.readValue(json)

        Assert.assertEquals(24, deserialized.clockFormat)
    }
}
