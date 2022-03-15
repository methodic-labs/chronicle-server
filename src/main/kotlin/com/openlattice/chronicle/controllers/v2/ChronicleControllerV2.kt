package com.openlattice.chronicle.controllers.v2

import com.codahale.metrics.annotation.Timed
import com.google.common.base.Optional
import com.google.common.collect.SetMultimap
import com.openlattice.chronicle.api.ChronicleApi
import com.openlattice.chronicle.data.ChronicleAppsUsageDetails
import com.openlattice.chronicle.data.LegacyChronicleQuestionnaire
import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.services.legacy.LegacyEdmResolver
import com.openlattice.chronicle.services.legacy.LegacyUtil
import com.openlattice.chronicle.services.settings.OrganizationSettingsManager
import com.openlattice.chronicle.services.studies.StudyService
import com.openlattice.chronicle.services.surveys.SurveysManager
import com.openlattice.chronicle.services.upload.AppDataUploadManager
import com.openlattice.chronicle.settings.AppComponent
import com.openlattice.chronicle.sources.SourceDevice
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.security.InvalidParameterException
import java.util.UUID
import javax.inject.Inject

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@RestController
@RequestMapping(ChronicleApi.CONTROLLER)
class ChronicleControllerV2 : ChronicleApi {
    @Inject
    private lateinit var dataUploadManager: AppDataUploadManager

    @Inject
    private lateinit var enrollmentManager: EnrollmentManager

    @Inject
    private lateinit var surveysManager: SurveysManager

    @Inject
    private lateinit var organizationSettingsManager: OrganizationSettingsManager

    @Inject
    private lateinit var studyService: StudyService

    @Timed
    @RequestMapping(
            path = [ChronicleApi.ORGANIZATION_ID_PATH + ChronicleApi.STUDY_ID_PATH + ChronicleApi.PARTICIPANT_ID_PATH + ChronicleApi.DATASOURCE_ID_PATH + ChronicleApi.ENROLL_PATH],
            method = [RequestMethod.POST], consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun enroll(
            @PathVariable(ChronicleApi.ORGANIZATION_ID) organizationId: UUID,
            @PathVariable(ChronicleApi.STUDY_ID) studyId: UUID,
            @PathVariable(ChronicleApi.PARTICIPANT_ID) participantId: String,
            @PathVariable(ChronicleApi.DATASOURCE_ID) datasourceId: String,
            @RequestBody datasource: Optional<SourceDevice>
    ): UUID {
        val realStudyId = studyService.getStudyId(studyId)
        checkNotNull(realStudyId) { "invalid study id" }
        if (datasource.isPresent) {
            return enrollmentManager.registerDatasource(
                    realStudyId,
                    participantId,
                    datasourceId,
                    datasource.get()
            )
        } else {
            throw InvalidParameterException("Datasource must be specified when enrolling.")
        }
    }

    @Timed
    @RequestMapping(
            path = [ChronicleApi.ORGANIZATION_ID_PATH + ChronicleApi.STUDY_ID_PATH + ChronicleApi.NOTIFICATIONS_PATH],
            method = [RequestMethod.GET]
    )
    override fun isNotificationsEnabled(
            @PathVariable(ChronicleApi.ORGANIZATION_ID) organizationId: UUID,
            @PathVariable(ChronicleApi.STUDY_ID) studyId: UUID
    ): Boolean {
        val realStudyId = studyService.getStudyId(studyId)
        checkNotNull(realStudyId) { "invalid study id" }
        return studyService.isNotificationsEnabled(realStudyId)
    }

    @Timed
    @RequestMapping(
            path = [ChronicleApi.ORGANIZATION_ID_PATH + ChronicleApi.STUDY_ID_PATH + ChronicleApi.PARTICIPANT_ID_PATH + ChronicleApi.ENROLLMENT_STATUS_PATH],
            method = [RequestMethod.GET]
    )
    override fun getParticipationStatus(
            @PathVariable(ChronicleApi.ORGANIZATION_ID) organizationId: UUID,
            @PathVariable(ChronicleApi.STUDY_ID) studyId: UUID,
            @PathVariable(ChronicleApi.PARTICIPANT_ID) participantId: String
    ): ParticipationStatus {
        val realStudyId = studyService.getStudyId(studyId)
        checkNotNull(realStudyId) { "invalid study id"}
        return enrollmentManager.getParticipationStatus(realStudyId, participantId)
    }

    @Timed
    @RequestMapping(
            path = [ChronicleApi.ORGANIZATION_ID_PATH + ChronicleApi.STUDY_ID_PATH + ChronicleApi.QUESTIONNAIRES_PATH],
            method = [RequestMethod.GET], produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getStudyQuestionnaires(
            @PathVariable(ChronicleApi.ORGANIZATION_ID) organizationId: UUID,
            @PathVariable(ChronicleApi.STUDY_ID) studyId: UUID
    ): Map<UUID, Map<FullQualifiedName, Set<Any>>> {
        val realStudyId = studyService.getStudyId(studyId)
        checkNotNull(realStudyId) { "invalid study id" }
        return surveysManager.getLegacyStudyQuestionnaires(organizationId, realStudyId)
    }

    @Timed
    @RequestMapping(
            path = [ChronicleApi.ORGANIZATION_ID_PATH + ChronicleApi.STUDY_ID_PATH + ChronicleApi.PARTICIPANT_ID_PATH + ChronicleApi.DATASOURCE_ID_PATH + ChronicleApi.UPLOAD_PATH],
            method = [RequestMethod.POST], consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun upload(
            @PathVariable(ChronicleApi.ORGANIZATION_ID) organizationId: UUID,
            @PathVariable(ChronicleApi.STUDY_ID) studyId: UUID,
            @PathVariable(ChronicleApi.PARTICIPANT_ID) participantId: String,
            @PathVariable(ChronicleApi.DATASOURCE_ID) datasourceId: String,
            @RequestBody data: List<SetMultimap<UUID, Any>>
    ): Int {
        val realStudyId = studyService.getStudyId(studyId)
        checkNotNull(realStudyId) { "invalid study id" }
        return dataUploadManager.upload(realStudyId, participantId, datasourceId, data)
    }

    @Timed
    @RequestMapping(
            path = [ChronicleApi.EDM_PATH], method = [RequestMethod.POST],
            consumes = [MediaType.APPLICATION_JSON_VALUE], produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getPropertyTypeIds(
            @RequestBody propertyTypeFqns: Set<FullQualifiedName>
    ): Map<FullQualifiedName, UUID> {
        return LegacyEdmResolver.getPropertyTypeIds(propertyTypeFqns)
    }

    @Timed
    @RequestMapping(path = [ChronicleApi.STATUS_PATH], method = [RequestMethod.GET])
    override fun isRunning(): Boolean {
        return true
    }
}
