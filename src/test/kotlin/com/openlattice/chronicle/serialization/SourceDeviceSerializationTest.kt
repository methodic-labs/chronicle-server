package com.openlattice.chronicle.serialization

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.core.type.TypeReference
import com.geekbeast.serializer.serializer.AbstractJacksonSerializationTest
import com.google.common.base.Optional
import com.openlattice.chronicle.sources.AndroidDevice
import com.openlattice.chronicle.sources.SourceDevice
import org.junit.Ignore

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Ignore //https://github.com/FasterXML/jackson-databind/issues/3390
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

