package com.openlattice.chronicle.survey

import com.google.common.collect.Iterables
import com.google.common.collect.Lists
import com.openlattice.chronicle.ChronicleServerTests
import com.openlattice.chronicle.client.ChronicleClient
import com.openlattice.chronicle.constants.EdmConstants
import com.openlattice.chronicle.data.FileType
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.QUESTION_TITLE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.RESPONSES
import com.openlattice.chronicle.util.tests.TestDataFactory
import org.dmfs.rfc5545.recur.RecurrenceRule
import org.junit.Assert
import org.junit.Test

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class QuestionnaireTests : ChronicleServerTests() {
    private val chronicleClient: ChronicleClient = clientUser1

    @Test
    fun testCreateQuestionnaire() {
        val studyApi = chronicleClient.studyApi
        val surveyApi = chronicleClient.surveyApi

        val questionnaire = questionnaire()
        val studyId = studyApi.createStudy(TestDataFactory.study())
        val questionnaireId = surveyApi.createQuestionnaire(studyId, questionnaire)

        val created = surveyApi.getQuestionnaire(studyId, questionnaireId)
        Assert.assertEquals(questionnaire.title, created.title)
        Assert.assertEquals(questionnaire.description, created.description)
        Assert.assertEquals(questionnaire.active, created.active)
        Assert.assertArrayEquals(questionnaire.questions.toTypedArray(), created.questions.toTypedArray())
        Assert.assertNotNull(created.dateCreated)
    }

    @Test
    fun testUpdateQuestionnaire() {
        val studyApi = chronicleClient.studyApi
        val surveyApi = chronicleClient.surveyApi

        val studyId = studyApi.createStudy(TestDataFactory.study())
        val questionnaire = questionnaire()
        val questionnaireId = surveyApi.createQuestionnaire(studyId, questionnaire)

        val update = QuestionnaireUpdate(
            title = "Updated Title",
            description = null,
            recurrenceRule = RecurrenceRule("FREQ=DAILY;BYHOUR=19;BYMINUTE=0;BYSECOND=0").toString(),
            active = false,
            questions = null
        )
        surveyApi.updateQuestionnaire(studyId, questionnaireId, update)
        val updated = surveyApi.getQuestionnaire(studyId, questionnaireId)

         Assert.assertEquals( update.title, updated.title)
        Assert.assertEquals(updated.description, questionnaire.description) //description field in update object, therefore description shouldn't be updated
        Assert.assertEquals(updated.recurrenceRule, update.recurrenceRule)
        Assert.assertEquals(updated.active, update.active)
        Assert.assertEquals(updated.questions, questionnaire.questions) //questions field in update object is null, so questions shouldn't be updated

    }

    @Test(expected = Exception::class)
    fun testDeleteQuestionnaire() {
        val studyApi = chronicleClient.studyApi
        val surveyApi = chronicleClient.surveyApi

        val studyId = studyApi.createStudy(TestDataFactory.study())
        val questionnaire = questionnaire()
        val questionnaireId = surveyApi.createQuestionnaire(studyId, questionnaire)

        surveyApi.deleteQuestionnaire(studyId, questionnaireId)
        surveyApi.getQuestionnaire(studyId, questionnaireId)
    }

    @Test
    fun testGetStudyQuestionnaires() {
        val studyApi = chronicleClient.studyApi
        val surveyApi = chronicleClient.surveyApi

        val studyId = studyApi.createStudy(TestDataFactory.study())

        surveyApi.createQuestionnaire(studyId, questionnaire())
        surveyApi.createQuestionnaire(studyId, questionnaire())

        val questionnaires = surveyApi.getStudyQuestionnaires(studyId)
        Assert.assertEquals(questionnaires.size, 2)
    }


    @Test
    fun testLegacyGetQuestionnaires() {
        val studyApi = chronicleClient.studyApi
        val surveyApi = chronicleClient.surveyApi
        val legacyStudyApi = chronicleClient.legacyChronicleStudyApi

        val studyId = studyApi.createStudy(TestDataFactory.study())
        val questionnaire = questionnaire()
        questionnaire.recurrenceRule = RecurrenceRule("FREQ=DAILY;BYHOUR=19;BYMINUTE=0;BYSECOND=0").toString()
        val questionnaireId = surveyApi.createQuestionnaire(studyId, questionnaire)

        val v3Questionnaire = surveyApi.getQuestionnaire(studyId, questionnaireId)
        val legacyQuestionnaires = legacyStudyApi.getStudyQuestionnaires(studyId)

        Assert.assertEquals(legacyQuestionnaires.size, 1)
        Assert.assertEquals(legacyQuestionnaires.keys.first(), questionnaireId)

        val legacyQuestionnaire = legacyQuestionnaires.getValue(questionnaireId)
        Assert.assertEquals(legacyQuestionnaire.getValue(EdmConstants.NAME_FQN).iterator().next().toString(), v3Questionnaire.title)
        Assert.assertEquals(legacyQuestionnaire.getValue(EdmConstants.DESCRIPTION_FQN).iterator().next().toString(), v3Questionnaire.description)
        Assert.assertEquals(legacyQuestionnaire.getValue(EdmConstants.ACTIVE_FQN).iterator().next() as Boolean, v3Questionnaire.active)
        Assert.assertEquals(legacyQuestionnaire.getValue(EdmConstants.RRULE_FQN).iterator().next().toString(), v3Questionnaire.recurrenceRule)
    }

    @Test(expected = Exception::class)
    fun testDuplicateQuestionsShouldThrow() {
        Questionnaire(
            id = null,
            title = "Bad questionnaire",
            description = "Questionnaire with duplicate titles",
            recurrenceRule = null,
            active = false,
            questions = listOf(
                Question("title1"),
                Question("title1")
            ),
            dateCreated = null
        )
    }

    @Test(expected = Exception::class)
    fun testEmptyQuestionsListShouldThrow(){
        Questionnaire(
            id = null,
            title = "Bad questionnaire",
            description = "Questionnaire with no questions",
            recurrenceRule = null,
            active = false,
            questions = listOf(),
            dateCreated = null
        )
    }

    @Test
    fun testQuestionnaireSubmission() {
        val studyApi = chronicleClient.studyApi
        val surveyApi = chronicleClient.surveyApi

        val studyId = studyApi.createStudy(TestDataFactory.study())
        val participant = TestDataFactory.participant()
        studyApi.registerParticipant(studyId, participant)

        val questionnaireId = surveyApi.createQuestionnaire(studyId, questionnaire())

        val response1 = QuestionnaireResponse(questionTitle = "Question 1", setOf("answer1", "answer2"))
        val response2 = QuestionnaireResponse(questionTitle = "Question 2", setOf("answer3"))
        val responses = listOf(response1, response2)

        surveyApi.submitQuestionnaireResponses(studyId, participant.participantId, questionnaireId, responses)
    }

    private fun questionnaire(): Questionnaire {
        return Questionnaire(
            id = null,
            title = "Test questionnaire",
            description = "test questionnaire",
            questions = listOf(
                Question(title = "Question 1"),
                Question(title = "Question 2", choices = setOf("choice 1", "choice 2", "choice 3"))
            ),
            dateCreated = null,
            recurrenceRule = null
        )
    }
}
