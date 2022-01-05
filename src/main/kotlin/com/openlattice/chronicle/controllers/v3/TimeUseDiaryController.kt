package com.openlattice.chronicle.controllers.v3
import com.openlattice.chronicle.api.TimeUseDiaryApi
import com.openlattice.chronicle.api.TimeUseDiaryApi.Companion.CONTROLLER
import com.openlattice.chronicle.api.TimeUseDiaryApi.Companion.DATA_TYPE
import com.openlattice.chronicle.api.TimeUseDiaryApi.Companion.END_DATE
import com.openlattice.chronicle.api.TimeUseDiaryApi.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.api.TimeUseDiaryApi.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.api.TimeUseDiaryApi.Companion.START_DATE
import com.openlattice.chronicle.api.TimeUseDiaryApi.Companion.STUDY_ID
import com.openlattice.chronicle.api.TimeUseDiaryApi.Companion.IDS_PATH
import com.openlattice.chronicle.api.TimeUseDiaryApi.Companion.ORGANIZATION_ID_PATH
import com.openlattice.chronicle.api.TimeUseDiaryApi.Companion.PARTICIPANT_ID_PATH
import com.openlattice.chronicle.api.TimeUseDiaryApi.Companion.STUDY_ID_PATH
import com.openlattice.chronicle.api.TimeUseDiaryApi.Companion.STATUS_PATH
import com.openlattice.chronicle.authorization.AuthorizingComponent
import com.codahale.metrics.annotation.Timed
import com.openlattice.chronicle.services.timeusediary.TimeUseDiaryManager
import com.openlattice.chronicle.services.timeusediary.TimeUseDiaryService
import com.openlattice.chronicle.tud.TudDownloadDataType
import com.openlattice.chronicle.tud.TudResponse
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import org.springframework.format.annotation.*
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.util.UUID
import java.time.OffsetDateTime
import java.util.Date
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
import javax.inject.Inject

/**
 * @author Andrew Carter andrew@openlattice.com
 */

@RestController
@RequestMapping(CONTROLLER)
class TimeUseDiaryController : TimeUseDiaryApi {
    @Inject
    private lateinit var timeUseDiaryManager: TimeUseDiaryManager

    companion object {
        private val logger = LoggerFactory.getLogger(TimeUseDiaryController::class.java)!!
    }

    @Timed
    @GetMapping(
        path = [ORGANIZATION_ID_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun downloadTimeUseDiaryData(
        @PathVariable(ORGANIZATION_ID) organizationId: UUID,
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        @RequestParam(DATA_TYPE) type: TudDownloadDataType,
        @RequestBody submissionIds: Set<UUID>
    ) {
        logger.info("OrganizationId $organizationId")
        logger.info("StudyId $studyId")
        logger.info("ParticipantId $participantId")
        logger.info("Type $type")
        logger.info("SubmissionIds $submissionIds")
    }

    @Timed
    @PostMapping(
        path = [ORGANIZATION_ID_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun submitTimeUseDiary(
        @PathVariable(ORGANIZATION_ID) organizationId: UUID,
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        @RequestBody responses: List<TudResponse>
    ): UUID {
        return timeUseDiaryManager.submitTimeUseDiary(
            organizationId,
            studyId,
            participantId,
            responses
        )
    }

    @Timed
    @GetMapping(
        path = [IDS_PATH + ORGANIZATION_ID_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getSubmissionsByDate(
        @PathVariable(ORGANIZATION_ID) organizationId: UUID,
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        @RequestParam(START_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) startDateTime: LocalDate,
        @RequestParam(END_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) endDateTime: LocalDate
    ): Map<LocalDate, Set<UUID>> {
        return timeUseDiaryManager.getSubmissionByDate(
            organizationId,
            studyId,
            participantId,
            startDateTime,
            endDateTime
        )
    }

    @Timed
    @GetMapping(
        path = [STATUS_PATH]
    )
    override fun isRunning(): Boolean {
        logger.info("TimeUseDiaryAPI is running...")
        return true
    }
}