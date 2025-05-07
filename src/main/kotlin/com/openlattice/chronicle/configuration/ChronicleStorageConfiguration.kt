package com.openlattice.chronicle.configuration

import com.openlattice.chronicle.storage.ChronicleStorage


/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class ChronicleStorageConfiguration(
    val defaultStorage: String = ChronicleStorage.PLATFORM.id,
    val platformStorage: String = ChronicleStorage.PLATFORM.id,
    val platformReadStorage: String = ChronicleStorage.PLATFORM_READ.id,
    val defaultEventStorage: String = ChronicleStorage.CHRONICLE.id,
    val auditStorage: String = ChronicleStorage.CHRONICLE.id,
)