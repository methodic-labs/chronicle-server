package com.openlattice.chronicle.controllers.legacy

import com.codahale.metrics.annotation.Timed
import com.google.common.collect.SetMultimap
import com.openlattice.chronicle.ChronicleApi
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.services.legacy.LegacyEdmResolver
import com.openlattice.chronicle.services.upload.AppDataUploadManager
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.util.*
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@RestController
@RequestMapping(ChronicleApi.CONTROLLER)
class ChronicleController : ChronicleApi {
    @Inject
    private lateinit var dataUploadManager: AppDataUploadManager

    @Inject
    private lateinit var enrollmentManager: EnrollmentManager

    @Timed
    @RequestMapping(
            path = [ChronicleApi.STUDY_ID_PATH + ChronicleApi.PARTICIPANT_ID_PATH + ChronicleApi.DATASOURCE_ID_PATH],
            method = [RequestMethod.POST], consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun upload(
            @PathVariable(ChronicleApi.STUDY_ID) studyId: UUID,
            @PathVariable(ChronicleApi.PARTICIPANT_ID) participantId: String,
            @PathVariable(ChronicleApi.DATASOURCE_ID) datasourceId: String,
            @RequestBody data: List<SetMultimap<UUID, Any>>
    ): Int {
        val organizationId = enrollmentManager.getOrganizationIdForLegacyStudy( studyId )
        return dataUploadManager.upload(organizationId, studyId, participantId, datasourceId, data)
    }

    @Timed
    @RequestMapping(
            path = [ChronicleApi.EDM_PATH], method = [RequestMethod.POST],
            consumes = [MediaType.APPLICATION_JSON_VALUE], produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getPropertyTypeIds(@RequestBody propertyTypeFqns: Set<String>): Map<String, UUID> {
        return LegacyEdmResolver.getLegacyPropertyTypeIds(propertyTypeFqns)
    }

    @Timed
    @RequestMapping(path = [ChronicleApi.STATUS_PATH], method = [RequestMethod.GET])
    override fun isRunning(): Boolean {
        return true
    }
}
