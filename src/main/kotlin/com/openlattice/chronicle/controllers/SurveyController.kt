package com.openlattice.chronicle.controllers

import com.codahale.metrics.annotation.Timed
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.AuthorizingComponent
import com.openlattice.chronicle.base.OK
import com.openlattice.chronicle.data.FileType
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.services.download.DataDownloadService
import com.openlattice.chronicle.services.surveys.SurveysService
import com.openlattice.chronicle.survey.*
import com.openlattice.chronicle.survey.SurveyApi.Companion.APP_USAGE_PATH
import com.openlattice.chronicle.survey.SurveyApi.Companion.CONTROLLER
import com.openlattice.chronicle.survey.SurveyApi.Companion.DATA_PATH
import com.openlattice.chronicle.survey.SurveyApi.Companion.END_DATE
import com.openlattice.chronicle.survey.SurveyApi.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.survey.SurveyApi.Companion.PARTICIPANT_ID_PATH
import com.openlattice.chronicle.survey.SurveyApi.Companion.PARTICIPANT_PATH
import com.openlattice.chronicle.survey.SurveyApi.Companion.QUESTIONNAIRE_ID
import com.openlattice.chronicle.survey.SurveyApi.Companion.QUESTIONNAIRE_ID_PATH
import com.openlattice.chronicle.survey.SurveyApi.Companion.QUESTIONNAIRE_PATH
import com.openlattice.chronicle.survey.SurveyApi.Companion.START_DATE
import com.openlattice.chronicle.survey.SurveyApi.Companion.STUDY_ID
import com.openlattice.chronicle.survey.SurveyApi.Companion.STUDY_ID_PATH
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
    val downloadService: DataDownloadService,
    val idGenerationService: HazelcastIdGenerationService,
    override val authorizationManager: AuthorizationManager,
    override val auditingManager: AuditingManager
) : SurveyApi, AuthorizingComponent {

    @Timed
    @GetMapping(
        path = [STUDY_ID_PATH + PARTICIPANT_PATH + PARTICIPANT_ID_PATH + APP_USAGE_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getAppUsageSurveyData(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        @RequestParam(value = START_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) startDateTime: OffsetDateTime,
        @RequestParam(value = END_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) endDateTime: OffsetDateTime
    ): List<AppUsage> {
        return surveysService.getAppUsageData(studyId, participantId, startDateTime, endDateTime)
    }

    @Timed
    @PostMapping(
        path = [STUDY_ID_PATH + PARTICIPANT_PATH + PARTICIPANT_ID_PATH + APP_USAGE_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
    )
    override fun submitAppUsageSurvey(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        @RequestBody surveyResponses: List<AppUsage>
    ) {
        surveysService.submitAppUsageSurvey(studyId, participantId, surveyResponses)
    }

    @Timed
    @PostMapping(
        path = [STUDY_ID_PATH + QUESTIONNAIRE_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun createQuestionnaire(
        @PathVariable(STUDY_ID) studyId: UUID,
        @RequestBody questionnaire: Questionnaire
    ): UUID {
        ensureWriteAccess(AclKey(studyId))
        val id = idGenerationService.getNextId()
        surveysService.createQuestionnaire(studyId, id, questionnaire)

        return id
    }

    @DeleteMapping(
        path = [STUDY_ID_PATH + QUESTIONNAIRE_PATH + QUESTIONNAIRE_ID_PATH]
    )
    override fun deleteQuestionnaire(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(QUESTIONNAIRE_ID) questionnaireId: UUID
    ): OK {
        ensureOwnerAccess(AclKey(studyId))
        surveysService.deleteQuestionnaire(studyId, questionnaireId)
        return OK("Successfully deleted questionnaire $questionnaireId")
    }

    @GetMapping(
        path = [STUDY_ID_PATH + QUESTIONNAIRE_PATH + QUESTIONNAIRE_ID_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getQuestionnaire(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(QUESTIONNAIRE_ID) questionnaireId: UUID
    ): Questionnaire {
        return surveysService.getQuestionnaire(studyId, questionnaireId)
    }

    @PatchMapping(
        path = [STUDY_ID_PATH + QUESTIONNAIRE_PATH + QUESTIONNAIRE_ID_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
    )
    override fun updateQuestionnaire(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(QUESTIONNAIRE_ID) questionnaireId: UUID,
        @RequestBody update: QuestionnaireUpdate
    ): OK {
        // TODO: ensure write access on study
        // ensureWriteAccess(AclKey(studyId))
        surveysService.updateQuestionnaire(studyId, questionnaireId, update)

        return OK("Successfully updated questionnaire $questionnaireId")
    }

    @GetMapping(
        path = [STUDY_ID_PATH + QUESTIONNAIRE_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getStudyQuestionnaires(@PathVariable(STUDY_ID) studyId: UUID): List<Questionnaire> {
        return surveysService.getStudyQuestionnaires(studyId)
    }

    @PostMapping(
        path = [STUDY_ID_PATH + PARTICIPANT_PATH + PARTICIPANT_ID_PATH + QUESTIONNAIRE_PATH + QUESTIONNAIRE_ID_PATH],
        produces = []
    )
    override fun submitQuestionnaireResponses(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        @PathVariable(QUESTIONNAIRE_ID) questionnaireId: UUID,
        @RequestBody responses: List<QuestionnaireResponse>
    ): OK {
        surveysService.submitQuestionnaireResponses(studyId, participantId, questionnaireId, responses)
        return OK()
    }

    override fun getQuestionnaireResponses(
        studyId: UUID,
        questionnaireId: UUID,
        fileType: FileType
    ): Iterable<Map<String, Any>> {
        // TODO: ensure read on study
       return downloadService.getQuestionnaireResponses(studyId, questionnaireId)
    }

    @GetMapping(
        path = [STUDY_ID_PATH + QUESTIONNAIRE_PATH + QUESTIONNAIRE_ID_PATH + DATA_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    fun downloadQuestionnaireResponses(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(QUESTIONNAIRE_ID) questionnaireId: UUID,
        @RequestParam(value = TYPE) fileType: FileType,
        httpServletResponse: HttpServletResponse
    ): Iterable<Map<String, Any>> {

        val data =  getQuestionnaireResponses(studyId, questionnaireId, fileType)
        //TODO: rename file
        val fileName = "Questionnaire_${questionnaireId}_${LocalDate.now().format( DateTimeFormatter.BASIC_ISO_DATE )}"

        ChronicleServerUtil.setDownloadContentType(httpServletResponse, fileType)
        ChronicleServerUtil.setContentDisposition(httpServletResponse, fileName, fileType)

        return data
    }
}
