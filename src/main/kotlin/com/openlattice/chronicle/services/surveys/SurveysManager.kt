package com.openlattice.chronicle.services.surveys

import com.openlattice.chronicle.data.ChronicleAppsUsageDetails
import com.openlattice.chronicle.data.ChronicleQuestionnaire
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.util.*


/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
interface SurveysManager {
    fun submitAppUsageSurvey(
            organizationId: UUID,
            studyId: UUID,
            participantId: String,
            associationDetails: Map<UUID, Map<FullQualifiedName, Set<Any>>>
    )

    fun getQuestionnaire(organizationId: UUID, studyId: UUID, questionnaireEKID: UUID): ChronicleQuestionnaire
    fun getStudyQuestionnaires(organizationId: UUID, studyId: UUID): Map<UUID, Map<FullQualifiedName, Set<Any>>>
    fun submitQuestionnaire(
            organizationId: UUID,
            studyId: UUID,
            participantId: String,
            questionnaireResponses: Map<UUID, Map<FullQualifiedName, Set<Any>>>
    )

    fun getParticipantAppsUsageData(
            organizationId: UUID, studyId: UUID, participantId: String, date: String
    ): List<ChronicleAppsUsageDetails>

    fun submitTimeUseDiarySurvey(
            organizationId: UUID, studyId: UUID, participantId: String,
            surveyData: List<Map<FullQualifiedName, Set<Any>>>
    )
}
