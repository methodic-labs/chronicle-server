package com.openlattice.chronicle.controllers.v3

import com.openlattice.chronicle.services.surveys.SurveysService
import com.openlattice.chronicle.survey.AppUsage
import com.openlattice.chronicle.survey.AppUsageSurveyApi
import com.openlattice.chronicle.survey.AppUsageSurveyApi.Companion.CONTROLLER
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.util.*
import javax.inject.Inject

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */

@RestController
@RequestMapping(CONTROLLER)
class AppUsageSurveyController: AppUsageSurveyApi {

    @Inject
    private lateinit var surveysService: SurveysService

    override fun getAppUsageData(organizationId: UUID, studyId: UUID, participantId: String, date: String): List<AppUsage> {
        return surveysService.getAppUsageData(organizationId, studyId, participantId, date)
    }

    override fun submitAppUsageSurvey(organizationId: UUID, studyId: UUID, participantId: String, surveyResponses: Map<UUID, Set<String>>) {
        surveysService.submitAppUsageSurvey(organizationId, studyId, participantId, surveyResponses)
    }
}