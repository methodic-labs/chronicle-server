package com.openlattice.chronicle.client

import com.openlattice.chronicle.candidates.CandidatesApi
import com.openlattice.chronicle.study.StudyApi

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class ChronicleClient (jwt: () -> String )  {
    private val retrofit = RetrofitClientFactory.newClient(Environment.TESTING_CHRONICLE, jwt)
    val studyApi: StudyApi = retrofit.create(StudyApi::class.java)
    val candidatesApi: CandidatesApi = retrofit.create(CandidatesApi::class.java)
}
