package com.openlattice.chronicle.services.surveys

import com.openlattice.chronicle.base.OK
import com.openlattice.chronicle.data.ChronicleAppsUsageDetails
import com.openlattice.chronicle.data.ChronicleQuestionnaire
import com.openlattice.chronicle.survey.AppUsage
import com.openlattice.chronicle.survey.Questionnaire
import com.openlattice.chronicle.survey.QuestionnaireResponse
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.time.OffsetDateTime
import java.util.*


/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
interface SurveysManager {

    fun getQuestionnaire(organizationId: UUID, studyId: UUID, questionnaireEKID: UUID): ChronicleQuestionnaire
    fun getStudyQuestionnaires(organizationId: UUID, studyId: UUID): Map<UUID, Map<FullQualifiedName, Set<Any>>>
    fun submitQuestionnaire(
            organizationId: UUID,
            studyId: UUID,
            participantId: String,
            questionnaireResponses: Map<UUID, Map<FullQualifiedName, Set<Any>>>
    )

    fun submitAppUsageSurvey(
            studyId: UUID,
            participantId: String,
            surveyResponses: List<AppUsage>
    )

    fun getAppUsageData(
            studyId: UUID,
            participantId: String,
            startDateTime: OffsetDateTime,
            endDateTime: OffsetDateTime
    ): List<AppUsage>

    fun createQuestionnaire(
        studyId: UUID,
        questionnaireId: UUID,
        questionnaire: Questionnaire
    )

    fun getQuestionnaire(
        studyId: UUID,
        questionnaireId: UUID
    ): Questionnaire

    fun toggleQuestionnaireStatus(
        studyId: UUID,
        questionnaireId: UUID
    )

    fun getStudyQuestionnaires(
        studyId: UUID
    ): List<Questionnaire>

    fun submitQuestionnaireResponses(
        studyId: UUID,
        participantId: String,
        questionnaireId: UUID,
        submissionId: UUID,
        responses: List<QuestionnaireResponse>
    )
}
