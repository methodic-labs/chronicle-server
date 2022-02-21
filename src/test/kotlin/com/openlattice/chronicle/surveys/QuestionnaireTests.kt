package com.openlattice.chronicle.surveys

import com.openlattice.chronicle.ChronicleServerTests
import com.openlattice.chronicle.client.ChronicleClient
import com.openlattice.chronicle.survey.Question
import com.openlattice.chronicle.survey.Questionnaire
import com.openlattice.chronicle.util.TestDataFactory
import org.dmfs.rfc5545.recur.RecurrenceRule
import org.junit.Assert
import org.junit.Before
import org.junit.Test

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class QuestionnaireTests : ChronicleServerTests() {
    private val chronicleClient: ChronicleClient = clientUser1

    private var questionnaire: Questionnaire? = null

    @Before
    fun beforeTests() {
        val title = "Test Questionnaire"
        val description = "test questionnaire description"
        val recurrenceRule = RecurrenceRule("FREQ=DAILY;BYHOUR=19;BYMINUTE=0;BYSECOND=0")
        questionnaire = Questionnaire(
            title = title,
            description = description,
            questions = listOf(
                Question("Example open ended question")
            ),
            id = null,
            recurrenceRule = recurrenceRule,
            dateCreated = null,
        )
    }

    @Test
    fun createQuestionnaire() {
        val studyApi = chronicleClient.studyApi
        val surveyApi = chronicleClient.surveyApi
        val study = TestDataFactory.study()
        val studyId = studyApi.createStudy(study)

        val questionnaireId = surveyApi.createQuestionnaire(studyId, questionnaire!!)

        val createdQuestionnaire = surveyApi.getQuestionnaire(studyId, questionnaireId)
        Assert.assertEquals(createdQuestionnaire.title, questionnaire?.title)
        Assert.assertEquals(createdQuestionnaire.description, questionnaire?.description)
//        Assert.assertEquals(createdQuestionnaire.recurrenceRule, questionnaire?.recurrenceRule)
        Assert.assertEquals(createdQuestionnaire.active, true)
    }

    @Test
    fun updateQuestionnaire() {

    }

}
