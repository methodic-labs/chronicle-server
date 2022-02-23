package com.openlattice.chronicle.controllers

import com.geekbeast.jdbc.DataSourceManager
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.hazelcast.core.HazelcastInstance
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.AuthorizingComponent
import com.openlattice.chronicle.candidates.Candidate
import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
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
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.sql.ResultSet
import java.time.LocalDate
import java.time.OffsetDateTime
import java.util.UUID

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
    companion object {
        private val logger = LoggerFactory.getLogger(ImportController::class.java)
    }

    @PostMapping(
        path = [STUDIES],
        produces = [MediaType.APPLICATION_JSON_VALUE],
        consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun importStudies(@RequestBody config: ImportStudiesConfiguration) {
        ensureAdminAccess()
        val hds = dataSourceManager.getDataSource(config.dataSourceName)
        val studiesByEkId = mutableMapOf<UUID, Study>()
        val studiesByLegacyStudyId = mutableMapOf<UUID, Study>()

        val studies = BasePostgresIterable(
            PreparedStatementHolderSupplier(hds, getStudiesSql(config.studiesTable)) {}
        ) {
            val study = study(it)
            val studyId = studyService.createStudy(study)

            logger.info("Created study {}",study)

            check(study.id == studyId) { "Safety check to make sure study id got set appropriately" }
            studiesByEkId[it.getObject(LEGACY_STUDY_EK_ID, UUID::class.java)] = study
            studiesByLegacyStudyId[it.getObject(LEGACY_STUDY_ID, UUID::class.java)] = study
        }.count()

        val participants = BasePostgresIterable(
            PreparedStatementHolderSupplier(hds, getCandidatesSql(config.candidatesTable)) {}
        ) {
            val participant = participant(it)
            val studyEkId = it.getObject(LEGACY_STUDY_EK_ID, UUID::class.java)

            val study = studiesByEkId[studyEkId] ?: throw StudyNotFoundException(
                studyEkId,
                "Missing study with legacy id $studyEkId"
            )
            studyService.registerParticipant(study.id, participant)
            logger.info("Registered participant {} in study {}", participant.participantId, study)
        }.count()
    }

    private fun participant(rs: ResultSet): Participant {
        var status = rs.getString(LEGACY_PARTICIPATION_STATUS)
        if (status == null || status == "DELETE") {
            status = ParticipationStatus.UNKNOWN.name
        }
        return Participant(
            participantId = rs.getString(LEGACY_PARTICIPANT_ID),
            candidate = Candidate(
                firstName = rs.getString(LEGACY_FIRST_NAME),
                lastName = rs.getString(LEGACY_LAST_NAME),
                dateOfBirth = rs.getObject(LEGACY_DOB, LocalDate::class.java)
            ),
            participationStatus = ParticipationStatus.valueOf(status)
        )
    }

    private fun study(rs: ResultSet): Study {

        val v2StudyId = rs.getString(LEGACY_STUDY_ID)
        val v2StudyEkid = rs.getString(LEGACY_STUDY_EK_ID)
        var title = rs.getString(LEGACY_TITLE)
        if (StringUtils.isBlank(title)) {
            title = "NO TITLE - POSSIBLY DELETED STUDY"
        }
        var description = rs.getString(LEGACY_DESC)
        if (StringUtils.isBlank(description)) {
            description = "study_id $v2StudyId study_ekid $v2StudyEkid"
        }
        return Study(
            title = title,
            description = description,
            group = rs.getString(LEGACY_STUDY_GROUP),
            version = rs.getString(LEGACY_STUDY_VERSION),
            contact = rs.getString(LEGACY_STUDY_CONTACT) ?: "",
            updatedAt = rs.getObject(LEGACY_UPDATE_AT, OffsetDateTime::class.java)
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
private const val LEGACY_STUDY_GROUP = "study_group"
private const val LEGACY_STUDY_VERSION = "study_version"
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
