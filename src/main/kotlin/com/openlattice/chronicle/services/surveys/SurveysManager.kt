package com.openlattice.chronicle.services.surveys

import com.openlattice.chronicle.data.LegacyChronicleQuestionnaire
import com.openlattice.chronicle.survey.AppUsage
import com.openlattice.chronicle.survey.Question
import com.openlattice.chronicle.survey.Questionnaire
import com.openlattice.chronicle.survey.QuestionnaireResponse
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.time.OffsetDateTime
import java.util.*


/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
interface SurveysManager {

    fun getLegacyQuestionnaire(organizationId: UUID, studyId: UUID, questionnaireEKID: UUID): LegacyChronicleQuestionnaire
    fun getLegacyStudyQuestionnaires(organizationId: UUID, studyId: UUID): Map<UUID, Map<FullQualifiedName, Set<Any>>>
    fun submitLegacyQuestionnaire(
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
        responses: List<QuestionnaireResponse>
    )
}
