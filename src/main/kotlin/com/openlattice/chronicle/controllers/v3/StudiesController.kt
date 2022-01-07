package com.openlattice.chronicle.controllers.v3

import com.codahale.metrics.annotation.Timed
import com.openlattice.chronicle.api.v3.StudiesApi
import com.openlattice.chronicle.api.v3.StudiesApi.Companion.CONTROLLER
import com.openlattice.chronicle.api.v3.StudiesApi.Companion.ORGANIZATION_ID_PATH
import com.openlattice.chronicle.services.studies.StudiesManager
import com.openlattice.chronicle.study.Study
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.util.*
import javax.inject.Inject


/**
 * @author Solomon Tang <solomon@openlattice.com>
 */

@RestController
@RequestMapping(CONTROLLER)
class StudiesController : StudiesApi {
    @Inject
    private lateinit var studiesManager: StudiesManager

    companion object {
        private val logger = LoggerFactory.getLogger(StudiesController::class.java)!!
    }

    @Timed
    @GetMapping(
        path = [ORGANIZATION_ID_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE],
    )
    override fun submitStudy(
        @PathVariable(StudiesApi.ORGANIZATION_ID) organizationId: UUID,
        @RequestBody study: Study
    ): UUID {
        logger.info("Submitting Study $study for organization $organizationId")
        return studiesManager.submitStudy(organizationId, study);
    }
}