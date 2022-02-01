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
class SourceDeviceSerializationTest : AbstractJacksonSerializationTest<Optional<SourceDevice>>() {

    override fun logResult(result: SerializationResult<Optional<SourceDevice>>) {
        logger.info("Actual JSON: ${result.jsonString}")
    }

    override fun getSampleData(): Optional<SourceDevice> {
        return Optional.of(
            AndroidDevice(
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
        )

    }

    override fun getClazz(): Class<Optional<SourceDevice>>? = null
    override fun getTypeReference(): TypeReference<Optional<SourceDevice>> =
        object : TypeReference<Optional<SourceDevice>>() {}

}