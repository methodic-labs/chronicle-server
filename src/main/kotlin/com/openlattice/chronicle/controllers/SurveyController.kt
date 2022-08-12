package com.openlattice.chronicle.controllers

import com.codahale.metrics.annotation.Timed
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.AuthorizingComponent
import com.openlattice.chronicle.base.OK
import com.openlattice.chronicle.base.OK.Companion.ok
import com.openlattice.chronicle.data.FileType
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.services.download.DataDownloadService
import com.openlattice.chronicle.services.studies.StudyService
import com.openlattice.chronicle.services.surveys.SurveysService
import com.openlattice.chronicle.study.StudySettingType
import com.openlattice.chronicle.survey.*
import com.openlattice.chronicle.survey.SurveyApi.Companion.APP_USAGE_PATH
import com.openlattice.chronicle.survey.SurveyApi.Companion.CONTROLLER
import com.openlattice.chronicle.survey.SurveyApi.Companion.DATA_PATH
import com.openlattice.chronicle.survey.SurveyApi.Companion.DEVICE_USAGE_PATH
import com.openlattice.chronicle.survey.SurveyApi.Companion.END_DATE
import com.openlattice.chronicle.survey.SurveyApi.Companion.FILE_NAME
import com.openlattice.chronicle.survey.SurveyApi.Companion.FILTERED_PATH
import com.openlattice.chronicle.survey.SurveyApi.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.survey.SurveyApi.Companion.PARTICIPANT_ID_PATH
import com.openlattice.chronicle.survey.SurveyApi.Companion.PARTICIPANT_PATH
import com.openlattice.chronicle.survey.SurveyApi.Companion.QUESTIONNAIRE_ID
import com.openlattice.chronicle.survey.SurveyApi.Companion.QUESTIONNAIRE_ID_PATH
import com.openlattice.chronicle.survey.SurveyApi.Companion.QUESTIONNAIRE_PATH
import com.openlattice.chronicle.survey.SurveyApi.Companion.START_DATE
import com.openlattice.chronicle.survey.SurveyApi.Companion.STUDY_ID
import com.openlattice.chronicle.survey.SurveyApi.Companion.STUDY_ID_PATH
import com.openlattice.chronicle.survey.SurveyApi.Companion.THRESHOLD
import com.openlattice.chronicle.survey.SurveyApi.Companion.TYPE
import com.openlattice.chronicle.util.ChronicleServerUtil
import org.springframework.format.annotation.DateTimeFormat
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import javax.inject.Inject
import javax.servlet.http.HttpServletResponse

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */

@RestController
@RequestMapping(CONTROLLER)
class SurveyController @Inject constructor(
    val surveysService: SurveysService,
    val studyService: StudyService,
    val downloadService: DataDownloadService,
    val idGenerationService: HazelcastIdGenerationService,
    override val authorizationManager: AuthorizationManager,
    override val auditingManager: AuditingManager,
) : SurveyApi, AuthorizingComponent {

    @Timed
    @GetMapping(
        path = [STUDY_ID_PATH + FILTERED_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getAppsFilteredForStudyAppUsageSurvey(@PathVariable(STUDY_ID) studyId: UUID): Collection<String> {
        ensureReadAccess(AclKey(studyId))
        return surveysService.getAppsFilteredForStudyAppUsageSurvey(studyId)
    }

    @Timed
    @PutMapping(
        path = [STUDY_ID_PATH + FILTERED_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun setAppsFilteredForStudyAppUsageSurvey(
        @PathVariable(STUDY_ID) studyId: UUID,
        @RequestBody appPackages: Set<String>,
    ): OK {
        ensureWriteAccess(AclKey(studyId))
        surveysService.setAppsFilteredForStudyAppUsageSurvey(studyId, appPackages)
        return ok
    }

    @Timed
    @PatchMapping(
        path = [STUDY_ID_PATH + FILTERED_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun filterAppForStudyAppUsageSurvey(
        @PathVariable(STUDY_ID) studyId: UUID,
        @RequestBody appPackages: Set<String>,
    ): OK {
        ensureWriteAccess(AclKey(studyId))
        surveysService.filterAppForStudyAppUsageSurvey(studyId, appPackages)
        return ok
    }

    @Timed
    @DeleteMapping(
        path = [STUDY_ID_PATH + FILTERED_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun allowAppForStudyAppUsageSurvey(studyId: UUID, @RequestBody appPackages: Set<String>): OK {
        ensureWriteAccess(AclKey(studyId))
        surveysService.allowAppForStudyAppUsageSurvey(studyId, appPackages)
        return ok
    }

    @Timed
    @GetMapping(
        path = [STUDY_ID_PATH + PARTICIPANT_PATH + PARTICIPANT_ID_PATH + DEVICE_USAGE_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getDeviceUsageSurveyData(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        @RequestParam(value = START_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) startDateTime: OffsetDateTime,
        @RequestParam(value = END_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) endDateTime: OffsetDateTime,
        @RequestParam(THRESHOLD) thresholdInSeconds: Int?,
    ): DeviceUsage {
        val realStudyId = studyService.getStudyId(studyId)
        checkNotNull(realStudyId) { "invalid study id" }
        val deviceUsageData = surveysService.getDeviceUsageData(realStudyId, participantId, startDateTime, endDateTime)

        val threshold = thresholdInSeconds ?: (studyService
            .getStudySettings(studyId)
            .getOrDefault(StudySettingType.Survey, SurveySettings()) as SurveySettings).appUsageThresholdInSeconds
        val packagesToKeep = deviceUsageData.usageByPackage.filterValues { it <= threshold }.keys
        val usageByPackage = deviceUsageData.usageByPackage - packagesToKeep
        return DeviceUsage(
            usageByPackage.values.sum(),
            usageByPackage,
            deviceUsageData.categoryByPackage - packagesToKeep
        )
    }

    @Timed
    @GetMapping(
        path = [STUDY_ID_PATH + PARTICIPANT_PATH + PARTICIPANT_ID_PATH + APP_USAGE_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getAppUsageSurveyData(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        @RequestParam(value = START_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) startDateTime: OffsetDateTime,
        @RequestParam(value = END_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) endDateTime: OffsetDateTime,
        @RequestParam(THRESHOLD) thresholdInSeconds: Int?,
    ): List<AppUsage> {
        val realStudyId = studyService.getStudyId(studyId)
        checkNotNull(realStudyId) { "invalid study id" }
        val appUsageData = surveysService.getAndroidAppUsageData(realStudyId, participantId, startDateTime, endDateTime)
        val aggregate = surveysService.computeAggregateUsage(appUsageData)

        val threshold = thresholdInSeconds ?: (studyService
            .getStudySettings(studyId)
            .getOrDefault(StudySettingType.Survey, SurveySettings()) as SurveySettings).appUsageThresholdInSeconds

        //Only keep packages that exceed threshold usage time for query.
        val packagesToKeep = aggregate.filterValues { it > threshold }.keys
        return appUsageData.filter { packagesToKeep.contains(it.appPackageName) }
    }

    @Timed
    @PostMapping(
        path = [STUDY_ID_PATH + PARTICIPANT_PATH + PARTICIPANT_ID_PATH + APP_USAGE_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
    )
    override fun submitAppUsageSurvey(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        @RequestBody surveyResponses: List<AppUsage>,
    ) {
        val realStudyId = studyService.getStudyId(studyId)
        checkNotNull(realStudyId) { "invalid study id" }
        surveysService.submitAppUsageSurvey(realStudyId, participantId, surveyResponses)
    }

    @Timed
    @PostMapping(
        path = [STUDY_ID_PATH + QUESTIONNAIRE_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun createQuestionnaire(
        @PathVariable(STUDY_ID) studyId: UUID,
        @RequestBody questionnaire: Questionnaire,
    ): UUID {
        ensureWriteAccess(AclKey(studyId))
        return surveysService.createQuestionnaire(studyId, questionnaire)
    }

    @Timed
    @DeleteMapping(
        path = [STUDY_ID_PATH + QUESTIONNAIRE_PATH + QUESTIONNAIRE_ID_PATH]
    )
    override fun deleteQuestionnaire(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(QUESTIONNAIRE_ID) questionnaireId: UUID,
    ): OK {
        ensureOwnerAccess(AclKey(studyId))
        surveysService.deleteQuestionnaire(studyId, questionnaireId)
        return OK("Successfully deleted questionnaire $questionnaireId")
    }

    @Timed
    @GetMapping(
        path = [STUDY_ID_PATH + QUESTIONNAIRE_PATH + QUESTIONNAIRE_ID_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getQuestionnaire(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(QUESTIONNAIRE_ID) questionnaireId: UUID,
    ): Questionnaire {
        return surveysService.getQuestionnaire(studyId, questionnaireId)
    }

    @Timed
    @PatchMapping(
        path = [STUDY_ID_PATH + QUESTIONNAIRE_PATH + QUESTIONNAIRE_ID_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
    )
    override fun updateQuestionnaire(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(QUESTIONNAIRE_ID) questionnaireId: UUID,
        @RequestBody update: QuestionnaireUpdate,
    ): OK {
        ensureWriteAccess(AclKey(studyId))
        surveysService.updateQuestionnaire(studyId, questionnaireId, update)

        return OK("Successfully updated questionnaire $questionnaireId")
    }

    @Timed
    @GetMapping(
        path = [STUDY_ID_PATH + QUESTIONNAIRE_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getStudyQuestionnaires(@PathVariable(STUDY_ID) studyId: UUID): List<Questionnaire> {
        val realStudyId = studyService.getStudyId(studyId)
        checkNotNull(realStudyId) { "invalid study id" }
        return surveysService.getStudyQuestionnaires(realStudyId)
    }

    @Timed
    @PostMapping(
        path = [STUDY_ID_PATH + PARTICIPANT_PATH + PARTICIPANT_ID_PATH + QUESTIONNAIRE_PATH + QUESTIONNAIRE_ID_PATH],
        produces = []
    )
    override fun submitQuestionnaireResponses(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        @PathVariable(QUESTIONNAIRE_ID) questionnaireId: UUID,
        @RequestBody responses: List<QuestionnaireResponse>,
    ): OK {
        val realStudyId = studyService.getStudyId(studyId)
        checkNotNull(realStudyId) { "invalid study id" }
        surveysService.submitQuestionnaireResponses(realStudyId, participantId, questionnaireId, responses)
        return OK()
    }

    override fun getQuestionnaireResponses(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(QUESTIONNAIRE_ID) questionnaireId: UUID,
        @PathVariable(TYPE) fileType: FileType,
    ): Iterable<Map<String, Any>> {
        ensureReadAccess(AclKey(studyId))
        return downloadService.getQuestionnaireResponses(studyId, questionnaireId)
    }

    @Timed
    @GetMapping(
        path = [STUDY_ID_PATH + QUESTIONNAIRE_PATH + QUESTIONNAIRE_ID_PATH + DATA_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    fun getQuestionnaireResponses(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(QUESTIONNAIRE_ID) questionnaireId: UUID,
        @RequestParam(value = TYPE) fileType: FileType,
        @RequestParam(value = FILE_NAME) fileName: String? = "Questionnaire_${questionnaireId}_${
            LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE)
        }",
        httpServletResponse: HttpServletResponse,
    ): Iterable<Map<String, Any>> {

        val data = getQuestionnaireResponses(studyId, questionnaireId, fileType)

        ChronicleServerUtil.setDownloadContentType(httpServletResponse, fileType)
        ChronicleServerUtil.setContentDisposition(httpServletResponse, fileName, fileType)

        return data
    }
}
