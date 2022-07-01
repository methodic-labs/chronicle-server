package com.openlattice.chronicle.services.jobs

import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.JOBS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.COMPLETED_AT
import com.openlattice.chronicle.storage.PostgresColumns.Companion.DELETED_ROWS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.JOB_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.UPDATED_AT
import java.sql.Connection

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
abstract class AbstractChronicleDeleteJobRunner<T : ChronicleJobDefinition>() : AbstractChronicleJobRunner<T>() {
    companion object {
        private val UPDATE_FINISHED_JOB_COLUMNS = listOf(
            UPDATED_AT,
            COMPLETED_AT,
            DELETED_ROWS
        ).joinToString(",") { it.name }

        private val UPDATE_FINISHED_DELETE_JOB_SQL = """
            UPDATE ${JOBS.name}
            SET ($UPDATE_FINISHED_JOB_COLUMNS) = (?, ?, ?)
            WHERE ${JOB_ID.name} = ?
        """.trimIndent()
    }

    fun updateFinishedDeleteJob(connection: Connection, job: ChronicleJob) {
        return connection.prepareStatement(UPDATE_FINISHED_DELETE_JOB_SQL).use { ps ->
            var index = 1
            ps.setObject(index++, job.updatedAt)
            ps.setObject(index++, job.completedAt)
            ps.setLong(index++, job.deletedRows)
            ps.setObject(index, job.id)
            ps.executeUpdate()
        }
    }

}