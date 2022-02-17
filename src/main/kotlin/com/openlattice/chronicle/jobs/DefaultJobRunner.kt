package com.openlattice.chronicle.jobs

import com.openlattice.chronicle.auditing.AuditableEvent
import org.slf4j.LoggerFactory
import java.sql.Connection

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class DefaultJobRunner<T : ChronicleJobDefinition>(private val clazz: Class<T>) : AbstractChronicleJobRunner<T>() {

    companion object {
        private val logger = LoggerFactory.getLogger(DefaultJobRunner::class.java)
        fun <T : ChronicleJobDefinition> getDefaultJobRunner(jobDefinition: T): ChronicleJobRunner<T> {
            return DefaultJobRunner(jobDefinition.javaClass)
        }
    }

    override fun runJob(connection: Connection, jobDefinition: ChronicleJob): List<AuditableEvent> {
        logger.warn("No job handler was found. Using default ")
        return listOf()
    }

    override fun accepts(): Class<T> {
        return clazz
    }
}