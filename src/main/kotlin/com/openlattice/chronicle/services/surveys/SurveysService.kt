package com.openlattice.chronicle.services.surveys

import com.openlattice.chronicle.data.ChronicleAppsUsageDetails
import com.openlattice.chronicle.data.ChronicleQuestionnaire
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class SurveysService(
        private val enrollmentManager: EnrollmentManager
) : SurveysManager {
    companion object {
        private val logger = LoggerFactory.getLogger(SurveysService::class.java)
        private val TIME_USE_DIARY_TITLE = "Time Use Diary"
    }

    override fun submitAppUsageSurvey(
            organizationId: UUID, studyId: UUID, participantId: String,
            associationDetails: Map<UUID, Map<FullQualifiedName, Set<Any>>>
    ) {
        TODO("Not yet implemented")
    }

    override fun getQuestionnaire(
            organizationId: UUID, studyId: UUID, questionnaireEKID: UUID
    ): ChronicleQuestionnaire {
        TODO("Not yet implemented")
    }

    override fun getStudyQuestionnaires(
            organizationId: UUID, studyId: UUID
    ): Map<UUID, Map<FullQualifiedName, Set<Any>>> {
        return mapOf()
    }

    override fun submitQuestionnaire(
            organizationId: UUID, studyId: UUID, participantId: String,
            questionnaireResponses: Map<UUID, Map<FullQualifiedName, Set<Any>>>
    ) {
        TODO("Not yet implemented")
    }

    override fun getParticipantAppsUsageData(
            organizationId: UUID, studyId: UUID, participantId: String, date: String
    ): List<ChronicleAppsUsageDetails> {
        TODO("Not yet implemented")
    }

    override fun submitTimeUseDiarySurvey(
            organizationId: UUID, studyId: UUID, participantId: String,
            surveyData: List<Map<FullQualifiedName, Set<Any>>>
    ) {
        TODO("Not yet implemented")
    }
}
