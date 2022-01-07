package com.openlattice.chronicle.services.studies

import com.openlattice.chronicle.storage.StorageResolver
import com.fasterxml.jackson.databind.ObjectMapper
import com.openlattice.chronicle.study.Study
import org.slf4j.LoggerFactory
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

        return studyId
    }
}