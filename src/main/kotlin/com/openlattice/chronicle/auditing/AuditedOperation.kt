package com.openlattice.chronicle.auditing

import org.slf4j.LoggerFactory
import java.sql.Connection

/**
 * This class makes it easy to perform transaction audited operations. As audit information can go into a separate
 * database, we opt for safety where you can record audit events for requests that may not have committed.
 *
 * If exception gets thrown, we can wrap and aud
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class AuditedOperation<R>(
    val connection: Connection,
    val op: (Connection) -> R,
    val auditOp: (R) -> List<AuditableEvent>,
    override val auditingManager: AuditingManager
) : AuditingComponent {
    companion object {
        private val logger = LoggerFactory.getLogger(AuditedOperation::class.java)
    }

    fun execute(): R {
        val result: R
        try {
            connection.autoCommit = false
            result = op(connection)
            recordEvents(auditOp(result))
            //We should stage audit events on postgres and then batch insert them into postgres
            /*
             * If an exception or service outage happens at this point, we will end up with an audit event for data
             * that wasn't actually committed.
             *
             * TODO: Stage audit events in postgres and then merge them into redshift in larger batches. This will
             * require adding a sequence or id column to ease merging audit events together.
             */

            connection.commit()
            connection.autoCommit = true
        } catch (ex: Exception) {
            connection.rollback()
            throw ex
        }

        return result
    }
}

class AuditedTransactionBuilder<R>(
    val connection: Connection,
    override val auditingManager: AuditingManager
) : AuditingComponent {
    private lateinit var op: (Connection) -> R
    private lateinit var auditOp: (R) -> List<AuditableEvent>

    fun transaction(op: (Connection) -> R): AuditedTransactionBuilder<R> {
        this.op = op
        return this
    }

    fun audit(auditOp: (R) -> List<AuditableEvent>): AuditedTransactionBuilder<R> {
        this.auditOp = auditOp
        return this
    }

    fun buildAndRun() = build().execute()

    fun build(): AuditedOperation<R> {
        check(this::op.isInitialized) { "Operation must be initialized." }
        check(this::auditOp.isInitialized) { "Audit operation must be initialized." }
        return AuditedOperation(connection, op, auditOp, auditingManager)
    }
}