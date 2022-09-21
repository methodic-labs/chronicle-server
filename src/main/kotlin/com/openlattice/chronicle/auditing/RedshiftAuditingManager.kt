package com.openlattice.chronicle.auditing

import com.geekbeast.mappers.mappers.ObjectMappers
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.AUDIT_BUFFER
import com.openlattice.chronicle.storage.RedshiftColumns
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.PRINCIPAL_TYPE
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.AUDIT
import com.openlattice.chronicle.storage.StorageResolver
import java.sql.PreparedStatement

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class RedshiftAuditingManager(private val storageResolver: StorageResolver) : AuditingManager {
    private val auditStorage = storageResolver.getAuditStorage()
    private val mapper = ObjectMappers.newJsonMapper()

    companion object {
        private val AUDIT_COLS = listOf(
            RedshiftColumns.ACL_KEY,
            RedshiftColumns.SECURABLE_PRINCIPAL_ID,
            RedshiftColumns.PRINCIPAL_TYPE,
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
         * 3. principal type
         * 4. principal id
         * 5. audit event type
         * 6. description
         * 7. study id
         * 8. organization id
         * 9. data
         * 10. timestamp
         */
        private val INSERT_AUDIT_SQL = """
        INSERT INTO ${AUDIT_BUFFER.name} ($AUDIT_COLS) VALUES (?,?,?,?,?,?,?,?,?,?)   
        """.trimIndent()
    }

    override fun recordEvents(events: List<AuditableEvent>): Int {
        return storageResolver.getPlatformStorage().connection.use { connection ->
            connection.prepareStatement(INSERT_AUDIT_SQL).use { ps ->
                events.forEach { event ->
                    bind(ps, event)
                    ps.addBatch()
                }
                ps.executeBatch().sum()
            }
        }
    }

    private fun bind(ps: PreparedStatement, event: AuditableEvent) {
        ps.setString(1, event.aclKey.index)
        ps.setString(2, event.securablePrincipalId.toString())
        ps.setString(3, event.principal.type.name)
        ps.setString(4, event.principal.id)
        ps.setString(5, event.eventType.name)
        ps.setString(6, event.description)
        ps.setString(7, event.study.toString())
        ps.setString(8, event.organization.toString())
        ps.setString(9, mapper.writeValueAsString(event.data))
        ps.setObject(10, event.timestamp)
    }
}