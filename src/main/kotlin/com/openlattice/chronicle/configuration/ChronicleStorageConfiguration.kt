package com.openlattice.chronicle.configuration

import com.openlattice.jdbc.DataSourceManager

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class ChronicleStorageConfiguration(
    val defaultStorage: String = DataSourceManager.DEFAULT_DATASOURCE,
    val auditStorage: String = "chronicle",
    val authorizationStorage: String = DataSourceManager.DEFAULT_DATASOURCE
)