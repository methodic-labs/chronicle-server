package com.openlattice.chronicle.controllers

import com.codahale.metrics.annotation.Timed
import com.openlattice.chronicle.services.surveys.SurveysService
import com.openlattice.chronicle.survey.AppUsage
import com.openlattice.chronicle.survey.AppUsageSurveyApi
import com.openlattice.chronicle.survey.AppUsageSurveyApi.Companion.CONTROLLER
import com.openlattice.chronicle.survey.AppUsageSurveyApi.Companion.END_DATE
import com.openlattice.chronicle.survey.AppUsageSurveyApi.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.survey.AppUsageSurveyApi.Companion.PARTICIPANT_ID_PATH
import com.openlattice.chronicle.survey.AppUsageSurveyApi.Companion.START_DATE
import com.openlattice.chronicle.survey.AppUsageSurveyApi.Companion.STUDY_ID
import com.openlattice.chronicle.survey.AppUsageSurveyApi.Companion.STUDY_ID_PATH
import org.springframework.format.annotation.DateTimeFormat
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.time.OffsetDateTime
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
            path = [STUDY_ID_PATH + PARTICIPANT_ID_PATH],
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
            path = [STUDY_ID_PATH + PARTICIPANT_ID_PATH],
            consumes = [MediaType.APPLICATION_JSON_VALUE],
    )
    override fun submitAppUsageSurvey(
            @PathVariable(STUDY_ID) studyId: UUID,
            @PathVariable(PARTICIPANT_ID) participantId: String,
            @RequestBody surveyResponses: List<AppUsage>
    ) {
        surveysService.submitAppUsageSurvey(studyId, participantId, surveyResponses)
    }
}
