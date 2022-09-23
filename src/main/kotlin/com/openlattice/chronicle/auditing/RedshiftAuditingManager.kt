package com.openlattice.chronicle.auditing

import com.geekbeast.configuration.postgres.PostgresFlavor
import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.postgres.PostgresDatatype
import com.geekbeast.util.StopWatch
import com.google.common.util.concurrent.MoreExecutors
import com.openlattice.chronicle.services.upload.AppDataUploadService
import com.openlattice.chronicle.storage.ChroniclePostgresTables
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.AUDIT_BUFFER
import com.openlattice.chronicle.storage.RedshiftColumns
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.PRINCIPAL_TYPE
import com.openlattice.chronicle.storage.RedshiftDataTables
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.AUDIT
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.buildMultilineInsertAuditEvents
import com.openlattice.chronicle.storage.StorageResolver
import org.slf4j.LoggerFactory
import java.security.InvalidParameterException
import java.sql.PreparedStatement
import java.time.OffsetDateTime
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.function.Supplier
import kotlin.math.min

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class RedshiftAuditingManager(private val storageResolver: StorageResolver) : AuditingManager {
    private val auditStorage = storageResolver.getAuditStorage()
    private val mapper = ObjectMappers.newJsonMapper()
    private val sempahore = Semaphore(10)

    companion object {
        private val executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1))
        private val logger = LoggerFactory.getLogger(RedshiftAuditingManager::class.java)
        private val AUDIT_COLS = AUDIT.columns.joinToString(",") { it.name }
        private const val RS_BATCH_SIZE = 32767 / 9 // 32767 / AUDIT.columns.size

        /**
         * 1. acl key
         * 2. securable principal id
         * 3. principal type
         * 4. principal id
         * 5. audit event type
         * 6. study id
         * 7. organization id
         * 8. description
         * 9. data
         * 10. timestamp
         */
        private val INSERT_AUDIT_SQL = """
        INSERT INTO ${AUDIT_BUFFER.name} ($AUDIT_COLS) VALUES (?,?,?,?,?,?,?,?,?,?)   
        """.trimIndent()

        private fun getMoveSql(batchSize: Int = 65536) = """
                DELETE FROM ${AUDIT_BUFFER.name} WHERE (${RedshiftColumns.STUDY_ID.name}, ${RedshiftColumns.PARTICIPANT_ID.name}) IN (
                    SELECT ${RedshiftColumns.TIMESTAMP.name}
                    FROM ${AUDIT_BUFFER.name}
                    ORDER BY ${RedshiftColumns.TIMESTAMP}
                    FOR UPDATE SKIP LOCKED
                    LIMIT $batchSize
                    )
                RETURNING *
                """.trimIndent()
    }

    init {
        executor.execute {
            while (true) {
                StopWatch("Moving audit {} events").use {
                    moveToRedshift()
                }
                Thread.sleep(60 * 1000)
            }
        }
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

    private fun moveToRedshift() :Int {
        val includeOnConflict = (auditStorage.first == PostgresFlavor.VANILLA)
        return try {
            if (!sempahore.tryAcquire()) return 0
            storageResolver.getPlatformStorage().connection.use { connection ->
                connection.autoCommit = false
                val auditEvents = mutableListOf<List<Any>>()
                connection.createStatement().executeQuery(getMoveSql(1024)).use { rs ->
                    while (rs.next()) {
                        auditEvents.add(AUDIT.columns.map { col ->
                            when (col.datatype) {
                                PostgresDatatype.TEXT_128,
                                PostgresDatatype.TEXT,
                                PostgresDatatype.TEXT_UUID,
                                PostgresDatatype.VARCHAR_MAX,
                                PostgresDatatype.TEXT_128 -> rs.getString(col.name)
                                PostgresDatatype.TIMESTAMPTZ -> rs.getObject(col.name, OffsetDateTime::class.java)
                                else -> throw InvalidParameterException("Unexpected column datatype ${col.datatype}")
                            }
                        })
                    }
                }
                val insertBatchSize = min(auditEvents.size, RS_BATCH_SIZE)
                val dr = auditEvents.size % RS_BATCH_SIZE

                val insertSql = buildMultilineInsertAuditEvents(insertBatchSize, includeOnConflict)

                val finalInsertSql = if (auditEvents.size > RS_BATCH_SIZE && dr != 0) {
                    logger.info("Preparing secondary insert statement with batch size $dr")
                    buildMultilineInsertAuditEvents(
                        dr,
                        includeOnConflict
                    )
                } else {
                    insertSql
                }

                //Commit to redshift and then commit delete. At some point we should make this so that duplicates are deleted from redshift audit log

                auditStorage.second.connection.use { auditConnection ->
                    auditConnection.autoCommit = false
                    val insertPs = auditConnection.prepareStatement(insertSql)
                    var finalPs = insertPs
                    auditEvents.chunked(RS_BATCH_SIZE).forEach { subList ->
                        val ps = if (subList.size == insertBatchSize) {
                            insertPs
                        } else {
                            finalPs = auditConnection.prepareStatement(finalInsertSql)
                            finalPs
                        }

                        subList.forEach { auditRow ->
                            auditRow.forEachIndexed { index, elem ->
                                when (elem) {
                                    is String -> ps.setString(index, elem)
                                    is OffsetDateTime -> ps.setObject(index, elem)
                                    else -> throw InvalidParameterException("Unexpected class in audit row.")
                                }
                            }
                        }

                        if (ps === insertPs)
                            ps.addBatch()
                    }

                    val movedRows =
                        insertPs.executeBatch().sum() + (if (finalPs !== insertPs) finalPs.executeUpdate() else 0)
                    logger.info("Moved $movedRows audit events to redshift.")
                    auditConnection.commit()
                    connection.commit()
                    auditConnection.autoCommit = true
                    connection.autoCommit = true
                    movedRows
                }
            }
        } catch (e: Exception) {
            logger.error("Unable to save data to redshift.", e)
            0
        } finally {
            sempahore.release()
        }
    }

    private fun bind(ps: PreparedStatement, event: AuditableEvent) {
        ps.setString(1, event.aclKey.index)
        ps.setString(2, event.securablePrincipalId.toString())
        ps.setString(3, event.principal.type.name)
        ps.setString(4, event.principal.id)
        ps.setString(5, event.eventType.name)
        ps.setString(6, event.study.toString())
        ps.setString(7, event.organization.toString())
        ps.setString(8, event.description)
        ps.setString(9, mapper.writeValueAsString(event.data))
        ps.setObject(10, event.timestamp)
    }
}