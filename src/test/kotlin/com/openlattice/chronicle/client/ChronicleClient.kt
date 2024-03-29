package com.openlattice.chronicle.client

import com.openlattice.chronicle.ChronicleStudyApi
import com.openlattice.chronicle.api.ChronicleApi
import com.openlattice.chronicle.candidates.CandidateApi
import com.openlattice.chronicle.organizations.OrganizationApi
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi
import com.openlattice.chronicle.study.StudyApi
import com.openlattice.chronicle.survey.SurveyApi

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class ChronicleClient(jwt: () -> String) {
    private val retrofit = RetrofitClientFactory.newClient(Environment.TESTING_CHRONICLE, jwt)
    val studyApi: StudyApi = retrofit.create(StudyApi::class.java)
    val timeUseDiaryApi: TimeUseDiaryApi = retrofit.create(TimeUseDiaryApi::class.java)
    val candidateApi: CandidateApi = retrofit.create(CandidateApi::class.java)
    val organizationApi: OrganizationApi = retrofit.create(OrganizationApi::class.java)
    val surveyApi: SurveyApi = retrofit.create(SurveyApi::class.java)
    val legacyChronicleStudyApi: ChronicleStudyApi = retrofit.create(ChronicleStudyApi::class.java)

    @Deprecated("This API is being deprecated.", level = DeprecationLevel.WARNING)
    val chronicleApi: ChronicleApi = retrofit.create(ChronicleApi::class.java)
}
