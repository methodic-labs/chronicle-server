package com.openlattice.chronicle.services.studies

import com.openlattice.chronicle.storage.StorageResolver
import com.fasterxml.jackson.databind.ObjectMapper
import com.openlattice.chronicle.study.Study
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.STUDIES
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.util.UUID

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
class StudiesService(
    private val storageResolver: StorageResolver
) : StudiesManager {

    companion object {
        private val logger = LoggerFactory.getLogger(StudiesService::class.java)
        private val objectMapper = ObjectMapper()
    }

    override fun submitStudy(
        organizationId: UUID,
        study: Study,
    ): UUID {
        val studyId = UUID.randomUUID()
        try {
            val (flavor, hds) = storageResolver.resolve(studyId)
            hds.connection.createStatement().execute(
                insertStudySql(
                    organizationId,
                    studyId,
                    study
                )
            )
        } catch (e: Exception) {
            logger.error("hds error: $e")
        }
        return studyId
    }

    private fun insertStudySql(
        organizationId: UUID,
        studyId: UUID,
        study: Study,
    ): String {

        return """
            INSERT INTO ${STUDIES.name} VALUES (
            '${organizationId},
            '${studyId}'
            '${study.title}',
            '${study.description}',
            '${LocalDate.now()}',
            '${LocalDate.now()}',
            '${study.startedAt}',
            '${study.endedAt}',
            '${study.group},
            '${study.version}')
        """.trimIndent()
    }
}