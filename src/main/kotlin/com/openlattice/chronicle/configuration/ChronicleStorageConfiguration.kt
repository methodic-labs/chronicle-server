package com.openlattice.chronicle.configuration

import com.geekbeast.jdbc.DataSourceManager


const val CHRONICLE_STORAGE = "chronicle"

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class ChronicleStorageConfiguration(
    val defaultStorage: String = DataSourceManager.DEFAULT_DATASOURCE,
    val platformStorage: String = DataSourceManager.DEFAULT_DATASOURCE,
    val defaultEventStorage: String = CHRONICLE_STORAGE,
    val auditStorage: String = CHRONICLE_STORAGE,
)