package com.openlattice.chronicle.jobs

import com.openlattice.chronicle.auditing.AuditableEvent
import java.sql.Connection

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
abstract class AbstractChronicleJobRunner<T : ChronicleJobDefinition> : ChronicleJobRunner<T> {
    final override fun run(connection: Connection, job: ChronicleJob): List<AuditableEvent> {
        check(accepts().isAssignableFrom(job.definition.javaClass)) {
            "Incompatible job of type ${job.definition.javaClass.name} for handler of type ${accepts().javaClass.name}"
        }
        return runJob(connection, job)
    }

    protected abstract fun runJob(connection: Connection, job: ChronicleJob): List<AuditableEvent>

}