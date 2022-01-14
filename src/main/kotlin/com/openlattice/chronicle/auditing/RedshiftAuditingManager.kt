package com.openlattice.chronicle.auditing

import com.dataloom.mappers.ObjectMappers
import com.openlattice.chronicle.storage.RedshiftColumns
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.AUDIT
import com.openlattice.chronicle.storage.StorageResolver
import java.sql.PreparedStatement

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class RedshiftAuditingManager(storageResolver: StorageResolver) : AuditingManager {
    private val auditStorage = storageResolver.getAuditStorage()
    private val mapper = ObjectMappers.newJsonMapper()

    companion object {
        private val AUDIT_COLS = listOf(
            RedshiftColumns.ACL_KEY,
            RedshiftColumns.SECURABLE_PRINCIPAL_ID,
            RedshiftColumns.PRINCIPAL_ID,
            RedshiftColumns.AUDIT_EVENT_TYPE,
            RedshiftColumns.DESCRIPTION,
            RedshiftColumns.STUDY_ID,
            RedshiftColumns.ORGANIZATION_ID,
            RedshiftColumns.DATA,
            RedshiftColumns.TIMESTAMP
        ).joinToString(",") { it.name }

        /**
         * 1. acl key
         * 2. securable principal id
         * 3. principal id
         * 4. audit event type
         * 5. description
         * 6. study id
         * 7. organization id
         * 8. data
         * 9. timestamp
         */
        private val INSERT_AUDIT_SQL = """
        INSERT INTO ${AUDIT.name} ($AUDIT_COLS) VALUES (?,?,?,?,?,?,?,?,?)   
        """.trimIndent()
    }

    override fun recordEvents(events: List<AuditableEvent>): Int {
        val (flavor, hds) = auditStorage
        return hds.connection.use { connection ->
            connection.prepareStatement(INSERT_AUDIT_SQL).use { ps ->
                events.forEach { event ->
                    bind( ps, event)
                    ps.addBatch()
                }
                ps.executeBatch().sum()
            }
        }
    }

    private fun bind(ps: PreparedStatement,event:AuditableEvent) {
        ps.setString( 1, event.aclKey.index)
        ps.setString(2, event.securablePrincipalId.toString())
        ps.setString(3, event.principalId)
        ps.setString(4, event.eventType.name)
        ps.setString(5, event.description)
        ps.setString(6, event.study.toString())
        ps.setString(7, event.organization.toString())
        ps.setString(8, mapper.writeValueAsString(event.data))
        ps.setObject(9, event.timestamp)
    }
}