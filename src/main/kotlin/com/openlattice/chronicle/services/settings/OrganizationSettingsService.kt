package com.openlattice.chronicle.services.settings

import com.fasterxml.jackson.module.kotlin.readValue
import com.geekbeast.mappers.mappers.ObjectMappers
import com.openlattice.chronicle.organizations.OrganizationSettings
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.LEGACY_STUDY_SETTINGS
import com.openlattice.chronicle.storage.PostgresColumns
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SETTINGS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.StorageResolver
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class OrganizationSettingsService(
    private val storageResolver: StorageResolver
) : OrganizationSettingsManager {
    private val mapper = ObjectMappers.getJsonMapper()

    companion object {
        /**
         * 1. study id
         */
        private val GET_LEGACY_ORGANIZATION_SETTINGS = """
            SELECT ${SETTINGS.name} FROM ${LEGACY_STUDY_SETTINGS.name} WHERE ${ORGANIZATION_ID.name} = ? 
        """
    }

    override fun getOrganizationSettings(organizationId: UUID): OrganizationSettings {
        return storageResolver.getPlatformStorage().connection.use { connection ->
            connection.prepareStatement(GET_LEGACY_ORGANIZATION_SETTINGS).use { ps ->
                ps.setObject(1, organizationId)
                ps.executeQuery().use { rs ->
                    if (rs.next()) mapper.readValue(rs.getString(SETTINGS.name)) else OrganizationSettings()
                }
            }
        }
    }
}