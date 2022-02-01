package com.openlattice.chronicle.serialization

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.guava.GuavaModule
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.geekbeast.serializer.serializer.AbstractJacksonSerializationTest
import com.google.common.base.Optional
import com.openlattice.chronicle.sources.AndroidDevice
import com.openlattice.chronicle.sources.SourceDevice

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class SourceDeviceSerializationTest : AbstractJacksonSerializationTest<SourceDevice>() {

    override fun logResult(result: SerializationResult<SourceDevice>) {
        logger.info("Actual JSON: ${result.jsonString}")
    }

    override fun getSampleData(): SourceDevice {
        val mapper2 = ObjectMapper()
        mapper2.registerModule(GuavaModule())
        mapper2.registerModule(JodaModule())
        mapper2.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
        val o =  Optional.of(AndroidDevice(
            "Samsung",
            "P",
            "Chocholate Chip",
            "Samsung",
            "21",
            "21",
            "",
            "",
            Optional.of(mutableMapOf()))
        ) as Optional<SourceDevice>

        val r =  mapper2.writeValueAsString(o)
        logger.info( "JSON: $r")
        return  mapper.readValue(r, object: TypeReference<Optional<AndroidDevice>> () {} ).get()
    }

    override fun getClazz(): Class<SourceDevice> {
        return SourceDevice::class.java
    }
}