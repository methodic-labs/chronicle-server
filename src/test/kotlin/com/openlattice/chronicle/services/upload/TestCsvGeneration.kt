package com.openlattice.chronicle.services.upload

import com.openlattice.chronicle.android.fromInteractionType
import org.apache.commons.lang3.RandomStringUtils
import org.junit.Test
import java.time.OffsetDateTime
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@getmethodic.com&gt;
 */
class TestCsvGeneration {


    @Test
    fun testCsv() {


        val event = UsageEventRow(
            UUID.randomUUID(),
            RandomStringUtils.randomAlphanumeric(10),
            "com.methodic.test",
            "Move to Foreground",
            fromInteractionType("Move to Foreground"),
            OffsetDateTime.now(),
            "America/Halifax",
            "child",
            "Chronicle",
            OffsetDateTime.now()
        )



        println(UsageEventCsvMapper.writeValueAsString(event))

    }


    @Test
    fun testMultipleLines() {

        val events = (1..10) .map {
            UsageEventRow(
                UUID.randomUUID(),
                RandomStringUtils.randomAlphanumeric(10),
                "com.methodic.test",
                "Move to Foreground",
                fromInteractionType("Move to Foreground"),
                OffsetDateTime.now(),
                "America/Halifax",
                "child",
                "Chronicle",
                OffsetDateTime.now()
            )
        }
        println(UsageEventCsvMapper.writeList(events))
    }
}