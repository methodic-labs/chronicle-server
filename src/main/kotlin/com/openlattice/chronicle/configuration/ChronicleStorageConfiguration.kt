package com.openlattice.chronicle.configuration

import com.geekbeast.jdbc.DataSourceManager
import com.openlattice.chronicle.storage.ChronicleStorage


/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class ChronicleStorageConfiguration(
    val defaultStorage: String = ChronicleStorage.PLATFORM.id,
    val platformStorage: String = ChronicleStorage.PLATFORM.id,
    val platformWorkStorage: String = ChronicleStorage.PLATFORM.id,
    val defaultEventStorage: String = ChronicleStorage.CHRONICLE.id,
    val auditStorage: String = ChronicleStorage.CHRONICLE.id,
)