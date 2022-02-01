package com.openlattice.chronicle.organizations

import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.Permission
import com.openlattice.chronicle.authorization.Principal
import com.openlattice.chronicle.authorization.SecurableObjectType
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.ORGANIZATIONS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.util.ensureVanilla
import java.sql.Connection
import java.util.EnumSet
import java.util.Optional
import java.util.UUID

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class ChronicleOrganizationService(
    private val storageResolver: StorageResolver,
    private val authorizationManager: AuthorizationManager

) {
    companion object {
        private val mapper = ObjectMappers.newJsonMapper()

        /**
         * 1. organization ids
         */
        private val GET_ORGANIZATION_SQL = """
            SELECT * FROM ${ORGANIZATIONS.name} WHERE ${ORGANIZATION_ID.name} = ANY(?)
        """.trimIndent()
        private val ORG_COLS = ORGANIZATIONS.columns.joinToString(",") { it.name }

        /**
         * 1. organization id
         * 2. title
         * 3. description
         * 4. settings
         */
        private val INSERT_ORGANIZATION_SQL = """
            INSERT INTO ${ORGANIZATIONS.name} ($ORG_COLS) VALUES (?,?,?,?::jsonb)
        """.trimIndent()
    }

    fun createOrganization(connection: Connection, owner: Principal, organization: Organization) {
        insertOrganization(connection,organization)
        authorizationManager.createSecurableObject(
            connection,
            AclKey(organization.id),
            owner,
            EnumSet.allOf(Permission::class.java),
            SecurableObjectType.Organization
        )


    }

    private fun insertOrganization(connection: Connection, organization: Organization) {
        connection.prepareStatement(INSERT_ORGANIZATION_SQL).use { ps ->
            ps.setObject(1, organization.id)
            ps.setString(2, organization.title)
            ps.setString(3, organization.description)
            ps.setObject(4, mapper.writeValueAsString(organization.settings))
            ps.executeUpdate()
        }
    }

    fun maybeGetOrganization(organizationId: UUID): Optional<Organization> {
        return Optional.ofNullable(getOrganizations(listOf(organizationId)).firstOrNull())
    }

    fun getOrganization(organizationId: UUID): Organization {
        return getOrganizations(listOf(organizationId)).first() //Since organization id is primary key in db, we're guaranteed one unique row
    }

    fun getOrganizations(organizationIds: Collection<UUID>): Iterable<Organization> {
        val (flavor, hds) = storageResolver.getDefaultPlatformStorage()
        ensureVanilla(flavor)
        return BasePostgresIterable(
            PreparedStatementHolderSupplier(
                hds,
                GET_ORGANIZATION_SQL
            ) { ps -> ps.setArray(1, PostgresArrays.createUuidArray(ps.connection, organizationIds)) }
        ) { ResultSetAdapters.organization(it) }
    }

}
