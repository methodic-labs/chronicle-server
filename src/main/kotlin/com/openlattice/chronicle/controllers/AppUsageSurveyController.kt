package com.openlattice.chronicle.controllers.v3

import com.codahale.metrics.annotation.Timed
import com.openlattice.chronicle.services.surveys.SurveysService
import com.openlattice.chronicle.survey.AppUsage
import com.openlattice.chronicle.survey.AppUsageSurveyApi
import com.openlattice.chronicle.survey.AppUsageSurveyApi.Companion.CONTROLLER
import com.openlattice.chronicle.survey.AppUsageSurveyApi.Companion.DATE
import com.openlattice.chronicle.survey.AppUsageSurveyApi.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.survey.AppUsageSurveyApi.Companion.ORGANIZATION_ID_PATH
import com.openlattice.chronicle.survey.AppUsageSurveyApi.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.survey.AppUsageSurveyApi.Companion.PARTICIPANT_ID_PATH
import com.openlattice.chronicle.survey.AppUsageSurveyApi.Companion.STUDY_ID
import com.openlattice.chronicle.survey.AppUsageSurveyApi.Companion.STUDY_ID_PATH
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.util.*
import javax.inject.Inject

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */

@RestController
@RequestMapping(CONTROLLER)
class AppUsageSurveyController : AppUsageSurveyApi {

    @Inject
    private lateinit var surveysService: SurveysService

    @Timed
    @GetMapping(
            path = [ORGANIZATION_ID_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getAppUsageData(
            @PathVariable(ORGANIZATION_ID) organizationId: UUID,
            @PathVariable(STUDY_ID) studyId: UUID,
            @PathVariable(PARTICIPANT_ID) participantId: String,
            @RequestParam(value = DATE) date: String
    ): List<AppUsage> {
        return surveysService.getAppUsageData(organizationId, studyId, participantId, date)
    }

    @Timed
    @PostMapping(
            path = [ORGANIZATION_ID_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH],
            consumes = [MediaType.APPLICATION_JSON_VALUE],
    )
    override fun submitAppUsageSurvey(
            @PathVariable(ORGANIZATION_ID) organizationId: UUID,
            @PathVariable(STUDY_ID) studyId: UUID,
            @PathVariable(PARTICIPANT_ID) participantId: String,
            @RequestBody surveyResponses: Map<UUID, Set<String>>
    ) {
        surveysService.submitAppUsageSurvey(organizationId, studyId, participantId, surveyResponses)
    }
}