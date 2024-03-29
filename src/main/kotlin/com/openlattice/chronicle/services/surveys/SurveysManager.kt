package com.openlattice.chronicle.services.surveys

import com.openlattice.chronicle.data.LegacyChronicleQuestionnaire
import com.openlattice.chronicle.survey.*
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.sql.Connection
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.*


/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
interface SurveysManager {

    fun getLegacyQuestionnaire(
        organizationId: UUID,
        studyId: UUID,
        questionnaireEKID: UUID,
    ): LegacyChronicleQuestionnaire

    fun getLegacyStudyQuestionnaires(organizationId: UUID, studyId: UUID): Map<UUID, Map<FullQualifiedName, Set<Any>>>
    fun submitLegacyQuestionnaire(
        organizationId: UUID,
        studyId: UUID,
        participantId: String,
        questionnaireResponses: Map<UUID, Map<FullQualifiedName, Set<Any>>>,
    )

    fun submitAppUsageSurvey(
        studyId: UUID,
        participantId: String,
        surveyResponses: List<AppUsage>,
    )

    fun getAndroidAppUsageData(
        studyId: UUID,
        participantId: String,
        startDateTime: OffsetDateTime,
        endDateTime: OffsetDateTime,
    ): List<AppUsage>

    fun createQuestionnaire(
        studyId: UUID,
        questionnaire: Questionnaire,
    ): UUID

    fun getQuestionnaire(
        studyId: UUID,
        questionnaireId: UUID,
    ): Questionnaire

    fun deleteQuestionnaire(
        studyId: UUID,
        questionnaireId: UUID,
    )

    fun updateQuestionnaire(
        studyId: UUID,
        questionnaireId: UUID,
        update: QuestionnaireUpdate,
    )

    fun getStudyQuestionnaires(
        studyId: UUID,
    ): List<Questionnaire>

    fun submitQuestionnaireResponses(
        studyId: UUID,
        participantId: String,
        questionnaireId: UUID,
        responses: List<QuestionnaireResponse>,
    )

    fun getAppsFilteredForStudyAppUsageSurvey(studyId: UUID): Collection<String>
    fun setAppsFilteredForStudyAppUsageSurvey(studyId: UUID, appPackages: Set<String>)
    fun filterAppForStudyAppUsageSurvey(studyId: UUID, appPackages: Set<String>)
    fun allowAppForStudyAppUsageSurvey(studyId: UUID, appPackages: Set<String>)
    fun initializeFilterdApps(connection: Connection, studyId: UUID)
    fun getDeviceUsageData(
        realStudyId: UUID,
        participantId: String,
        startDateTime: OffsetDateTime,
        endDateTime: OffsetDateTime,
    ): DeviceUsage

    fun computeAggregateUsage(
        startDateTime: OffsetDateTime = OffsetDateTime.now()
            .toLocalDate()
            .atStartOfDay(ZoneOffset.UTC.normalized())
            .toOffsetDateTime(),
        appUsage: List<AppUsage>,
    ): Map<String, Double>
}
