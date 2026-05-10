package com.openlattice.chronicle.auditing

import com.geekbeast.configuration.postgres.PostgresFlavor
import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.PostgresDatatype
import com.geekbeast.util.StopWatch
import com.google.common.util.concurrent.MoreExecutors
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.AUDIT_BUFFER
import com.openlattice.chronicle.storage.RedshiftColumns
import com.openlattice.chronicle.storage.PostgresDataTables
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.AUDIT
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.buildMultilineInsertAuditEvents
import com.openlattice.chronicle.storage.StorageResolver
import org.slf4j.LoggerFactory
import java.security.InvalidParameterException
import java.sql.PreparedStatement
import java.time.OffsetDateTime
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
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
        private const val RS_BATCH_SIZE = 32767 / 10 // 32767 / AUDIT.columns.size

        /**
         * Index of acl_key in the ordered AUDIT column list. Used to special-case
         * the hex-concat → uuid[] transformation when writing to the Postgres audit
         * table (which stores acl_key as uuid[] rather than varchar).
         */
        private val ACL_KEY_INDEX = AUDIT.columns.toList().indexOfFirst { it.name == RedshiftColumns.ACL_KEY.name }

        /**
         * Rebuilds the hyphen-stripped 32-char-per-uuid concat form written by
         * AclKey.index into a list of UUIDs for a real Postgres uuid[] array bind
         * (via PostgresArrays.createUuidArray). Empty input yields an empty list.
         */
        private fun aclKeyHexToUuids(concatHex: String): List<UUID> {
            if (concatHex.isEmpty()) return emptyList()
            require(concatHex.length % 32 == 0) {
                "acl_key index length must be a multiple of 32, got ${concatHex.length}"
            }
            return concatHex.chunked(32).map { hex ->
                UUID.fromString(
                    "${hex.substring(0, 8)}-${hex.substring(8, 12)}-${hex.substring(12, 16)}-${hex.substring(16, 20)}-${hex.substring(20, 32)}"
                )
            }
        }

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
                DELETE FROM ${AUDIT_BUFFER.name} WHERE ${RedshiftColumns.TIMESTAMP.name} IN (
                    SELECT ${RedshiftColumns.TIMESTAMP.name}
                    FROM ${AUDIT_BUFFER.name}
                    ORDER BY ${RedshiftColumns.TIMESTAMP.name}
                    FOR UPDATE SKIP LOCKED
                    LIMIT $batchSize
                    )
                RETURNING *
                """.trimIndent()
    }

    init {
        executor.execute {
            while (true) {
                StopWatch("Moving audit events").use {
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

    private fun moveToRedshift(): Int {
        val includeOnConflict = (auditStorage.first == PostgresFlavor.VANILLA)
        return try {
            if (!sempahore.tryAcquire()) return 0
            storageResolver.getPlatformStorage().connection.use { connection ->
                connection.autoCommit = false
                val auditEvents = mutableListOf<List<Any?>>()
                connection.createStatement().executeQuery(getMoveSql(1024)).use { rs ->
                    while (rs.next()) {
                        auditEvents.add(AUDIT.columns.map { col ->
                            when (col.datatype) {
                                PostgresDatatype.TEXT_128,
                                PostgresDatatype.TEXT,
                                PostgresDatatype.TEXT_UUID,
                                PostgresDatatype.VARCHAR_MAX,
                                PostgresDatatype.TEXT_256 -> rs.getString(col.name)
                                PostgresDatatype.TIMESTAMPTZ -> rs.getObject(col.name, OffsetDateTime::class.java)
                                else -> throw InvalidParameterException("Unexpected column datatype ${col.datatype}")
                            }
                        })
                    }
                }
                if (auditEvents.isEmpty()) {
                    connection.commit()
                    connection.autoCommit = true
                    return 0
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

                // Write to Redshift (existing path) — only when audit storage is actually Redshift.
                // The Redshift SQL builder emits untyped `?` placeholders and bind-as-string for all
                // columns, which is incompatible with the Aurora audit table where acl_key is uuid[].
                // Once audit storage is migrated to Postgres flavor, the dual-write block below
                // handles the Postgres path with a real array bind.
                if (auditStorage.first == PostgresFlavor.REDSHIFT) try {
                    auditStorage.second.connection.use { auditConnection ->
                        auditConnection.autoCommit = false
                        val insertPs = auditConnection.prepareStatement(insertSql)
                        var finalPs: PreparedStatement? = null
                        auditEvents.chunked(RS_BATCH_SIZE).forEach { subList ->
                            val ps = if (subList.size == insertBatchSize) {
                                insertPs
                            } else {
                                finalPs = auditConnection.prepareStatement(finalInsertSql)
                                finalPs
                            }!!
                            var indexBase = 0
                            subList.forEach { auditRow ->
                                auditRow.forEachIndexed { index, elem ->
                                    val pgIndex = indexBase + index + 1
                                    when (elem) {
                                        is String -> ps.setString(pgIndex, elem)
                                        is OffsetDateTime -> ps.setObject(pgIndex, elem)
                                        else -> throw InvalidParameterException("Unexpected class in audit row.")
                                    }
                                }
                                indexBase += AUDIT.columns.size
                            }

                            if (ps === insertPs)
                                ps.addBatch()
                        }

                        val movedRows = insertPs.executeBatch().sum() + (finalPs?.executeUpdate() ?: 0)
                        logger.info("Moved $movedRows audit events to redshift.")
                        auditConnection.commit()
                        movedRows
                    }
                } catch (e: Exception) {
                    logger.error("Failed to write audit events to Redshift.", e)
                }

                // Write to Postgres (new dual-write path)
                val pgInsertSql = PostgresDataTables.buildMultilineInsertAuditEvents(insertBatchSize)
                val pgFinalInsertSql = if (auditEvents.size > RS_BATCH_SIZE && dr != 0) {
                    PostgresDataTables.buildMultilineInsertAuditEvents(dr)
                } else {
                    pgInsertSql
                }

                try {
                    storageResolver.getPlatformStorage().connection.use { pgConnection ->
                        pgConnection.autoCommit = false
                        val pgInsertPs = pgConnection.prepareStatement(pgInsertSql)
                        var pgFinalPs: PreparedStatement? = null
                        auditEvents.chunked(RS_BATCH_SIZE).forEach { subList ->
                            val ps = if (subList.size == insertBatchSize) {
                                pgInsertPs
                            } else {
                                pgFinalPs = pgConnection.prepareStatement(pgFinalInsertSql)
                                pgFinalPs
                            }!!
                            var indexBase = 0
                            subList.forEach { auditRow ->
                                auditRow.forEachIndexed { index, elem ->
                                    val pgIndex = indexBase + index + 1
                                    when {
                                        index == ACL_KEY_INDEX && elem is String ->
                                            ps.setArray(
                                                pgIndex,
                                                PostgresArrays.createUuidArray(ps.connection, aclKeyHexToUuids(elem))
                                            )
                                        elem is String -> ps.setString(pgIndex, elem)
                                        elem is OffsetDateTime -> ps.setObject(pgIndex, elem)
                                        else -> throw InvalidParameterException("Unexpected class in audit row.")
                                    }
                                }
                                indexBase += AUDIT.columns.size
                            }

                            if (ps === pgInsertPs)
                                ps.addBatch()
                        }

                        val pgMovedRows = pgInsertPs.executeBatch().sum() + (pgFinalPs?.executeUpdate() ?: 0)
                        logger.info("Moved $pgMovedRows audit events to Postgres.")
                        pgConnection.commit()
                        pgConnection.autoCommit = true
                    }
                } catch (e: Exception) {
                    logger.error("Failed to write audit events to Postgres.", e)
                }

                connection.commit()
                connection.autoCommit = true
                auditEvents.size
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