package com.openlattice.chronicle.controllers

import com.geekbeast.jdbc.DataSourceManager
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.hazelcast.core.HazelcastInstance
import com.openlattice.chronicle.auditing.AuditEventType
import com.openlattice.chronicle.auditing.AuditableEvent
import com.openlattice.chronicle.auditing.AuditedOperationBuilder
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.AuthorizingComponent
import com.openlattice.chronicle.candidates.Candidate
import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.ids.IdConstants
import com.openlattice.chronicle.import.ImportApi
import com.openlattice.chronicle.import.ImportApi.Companion.CONTROLLER
import com.openlattice.chronicle.import.ImportApi.Companion.STUDIES
import com.openlattice.chronicle.import.ImportStudiesConfiguration
import com.openlattice.chronicle.participants.Participant
import com.openlattice.chronicle.services.candidates.CandidateService
import com.openlattice.chronicle.services.studies.StudyService
import com.openlattice.chronicle.services.timeusediary.TimeUseDiaryService
import com.openlattice.chronicle.services.upload.AppDataUploadService
import com.openlattice.chronicle.study.Study
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.sql.ResultSet
import java.time.LocalDate

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@RestController
@RequestMapping(CONTROLLER)
class ImportController(
    private val studyService: StudyService,
    private val candidateService: CandidateService,
    private val timeUseDiaryService: TimeUseDiaryService,
    private val appDataUploadService: AppDataUploadService,
    private val idGenerationService: HazelcastIdGenerationService,
    private val dataSourceManager: DataSourceManager,
    override val authorizationManager: AuthorizationManager,
    override val auditingManager: AuditingManager,
    hazelcast: HazelcastInstance
) : ImportApi, AuthorizingComponent {


    @PostMapping(
        path = [STUDIES],
        produces = [MediaType.APPLICATION_JSON_VALUE],
        consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun importStudies(config: ImportStudiesConfiguration) {
        ensureAdminAccess()
        val hds = dataSourceManager.getDataSource(config.dataSourceName)

        val participants = BasePostgresIterable(
            PreparedStatementHolderSupplier(hds, getCandidatesSql(config.candidatesTable)) {}
        ) { participant(it) }

        val studies = BasePostgresIterable(
            PreparedStatementHolderSupplier(hds, getStudiesSql(config.candidatesTable)) {}
        ) { study(it) }.forEach(studyService::createStudy)

    }

    private fun participant(rs: ResultSet): Participant {
        return Participant(
            participantId = rs.getString(LEGACY_PARTICIPANT_ID),
            candidate = Candidate(
                firstName = rs.getString(LEGACY_FIRST_NAME),
                lastName = rs.getString(LEGACY_LAST_NAME),
                dateOfBirth = rs.getObject(LEGACY_DOB, LocalDate::class.java)
            ),
            participationStatus = ParticipationStatus.valueOf(rs.getString(LEGACY_PARTICIPATION_STATUS))
        )
    }

    private fun study(rs: ResultSet): Study {
        val studyId = idGenerationService.getNextId()

        return Study(
            title = rs.getString(LEGACY_TITLE),
            group = rs.getString(LEGACY_STUDY_GROUP),
            version = rs.getString(LEGACY_STUDY_VERSION),
            contact = rs.getString(LEGACY_STUDY_CONTACT)
        )
    }
}

private const val LEGACY_FIRST_NAME = "first_name"
private const val LEGACY_LAST_NAME = "last_name"
private const val LEGACY_DOB = "dob"
private const val LEGACY_PARTICIPANT_ID = "participant_id"
private const val LEGACY_PARTICIPATION_STATUS = "participation_status"
private const val LEGACY_STUDY_ID = "v2_study_id"
private const val LEGACY_STUDY_EK_ID = "v2_study_ekid"
private const val LEGACY_TITLE = "title"
private const val LEGACY_DESC = "description"
private const val LEGACY_LAT = "lat"
private const val LEGACY_LON = "lon"
private const val LEGACY_UPDATE_AT = "updated_at"
private const val LEGACY_STUDY_GROUP = ""
private const val LEGACY_STUDY_VERSION = ""
private const val LEGACY_STUDY_CONTACT = "contact"


private fun getStudiesSql(studiesTable: String): String {
    return """
        SELECT * FROM $studiesTable
    """.trimIndent()
}

private fun getCandidatesSql(candidateTable: String): String {
    return """
        SELECT * FROM $candidateTable
    """.trimIndent()
}