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
import com.codahale.metrics.annotation.Timed
import com.openlattice.chronicle.services.timeusediary.TimeUseDiaryManager
import com.openlattice.chronicle.tud.TimeUseDiaryDownloadDataType
import com.openlattice.chronicle.tud.TimeUseDiaryResponse
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import org.springframework.format.annotation.*
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.UUID
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
    @PostMapping(
        path = [ORGANIZATION_ID_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun submitTimeUseDiary(
        @PathVariable(ORGANIZATION_ID) organizationId: UUID,
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        @RequestBody responses: List<TimeUseDiaryResponse>
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
        @RequestParam(START_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) startDateTime: LocalDateTime,
        @RequestParam(END_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) endDateTime: LocalDateTime
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
        path = [ORGANIZATION_ID_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun downloadTimeUseDiaryData(
        @PathVariable(ORGANIZATION_ID) organizationId: UUID,
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        @RequestParam(DATA_TYPE) type: TimeUseDiaryDownloadDataType,
        @RequestBody submissionIds: Set<UUID>
    ) {
        TODO("Not yet implemented")
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