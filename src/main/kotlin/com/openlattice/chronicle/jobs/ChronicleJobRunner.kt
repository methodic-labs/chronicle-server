package com.openlattice.chronicle.jobs

import com.openlattice.chronicle.auditing.AuditableEvent
import java.sql.Connection
import java.util.concurrent.Callable

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface ChronicleJobRunner<T : ChronicleJobDefinition> {
    /**
     * Validates and runs a job. The default behavior should be that the job definition class must be
     * assignable to the class returned by [accepts()]
     * @param job The job to run.
     *
     */
    fun run(connection: Connection, job : ChronicleJob ) : List<AuditableEvent>

    /**
     * The type of job definitions that can be run by this class.
     */
    fun accepts() : Class<T>
}