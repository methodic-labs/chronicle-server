package com.openlattice.chronicle.controllers

import com.codahale.metrics.annotation.Timed
import com.openlattice.chronicle.api.TimeUseDiaryApi
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.AuthorizingComponent
import com.openlattice.chronicle.authorization.Permission
import com.openlattice.chronicle.services.timeusediary.TimeUseDiaryManager
import com.openlattice.chronicle.tud.TimeUseDiaryDownloadDataType
import com.openlattice.chronicle.tud.TimeUseDiaryResponse
import org.slf4j.LoggerFactory
import org.springframework.format.annotation.DateTimeFormat
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.*
import javax.inject.Inject

/**
 * @author Andrew Carter andrew@openlattice.com
 */

@RestController
@RequestMapping(TimeUseDiaryApi.CONTROLLER)
class TimeUseDiaryController(
    override val authorizationManager: AuthorizationManager,
    override val auditingManager: AuditingManager
) : TimeUseDiaryApi, AuthorizingComponent {

    @Inject
    private lateinit var timeUseDiaryManager: TimeUseDiaryManager

    companion object {
        private val logger = LoggerFactory.getLogger(TimeUseDiaryController::class.java)!!
        private const val pstOffset = "-08:00"
    }

    @Timed
    @PostMapping(
        path = [TimeUseDiaryApi.ORGANIZATION_ID_PATH + TimeUseDiaryApi.STUDY_ID_PATH + TimeUseDiaryApi.PARTICIPANT_ID_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun submitTimeUseDiary(
        @PathVariable(TimeUseDiaryApi.ORGANIZATION_ID) organizationId: UUID,
        @PathVariable(TimeUseDiaryApi.STUDY_ID) studyId: UUID,
        @PathVariable(TimeUseDiaryApi.PARTICIPANT_ID) participantId: String,
        @RequestBody responses: List<TimeUseDiaryResponse>
    ): UUID {
        ensureAuthenticated()
        return timeUseDiaryManager.submitTimeUseDiary(
            organizationId,
            studyId,
            participantId,
            responses
        )
    }

    @Timed
    @GetMapping(
        path = [TimeUseDiaryApi.IDS_PATH + TimeUseDiaryApi.ORGANIZATION_ID_PATH + TimeUseDiaryApi.STUDY_ID_PATH + TimeUseDiaryApi.PARTICIPANT_ID_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getSubmissionsByDate(
        @PathVariable(TimeUseDiaryApi.ORGANIZATION_ID) organizationId: UUID,
        @PathVariable(TimeUseDiaryApi.STUDY_ID) studyId: UUID,
        @PathVariable(TimeUseDiaryApi.PARTICIPANT_ID) participantId: String,
        @RequestParam(TimeUseDiaryApi.START_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) startDateTime: LocalDateTime,
        @RequestParam(TimeUseDiaryApi.END_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) endDateTime: LocalDateTime,
        @RequestParam(value = TimeUseDiaryApi.ZONE_OFFSET, defaultValue = pstOffset) @DateTimeFormat(pattern = "Z") zoneOffset: ZoneOffset
    ): Map<LocalDate, Set<UUID>> {
        accessCheck(AclKey(studyId), EnumSet.of(Permission.READ))
        return timeUseDiaryManager.getSubmissionByDate(
            organizationId,
            studyId,
            participantId,
            startDateTime,
            endDateTime,
            zoneOffset
        )
    }

    @Timed
    @GetMapping(
        path = [TimeUseDiaryApi.ORGANIZATION_ID_PATH + TimeUseDiaryApi.STUDY_ID_PATH + TimeUseDiaryApi.PARTICIPANT_ID_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun downloadTimeUseDiaryData(
        @PathVariable(TimeUseDiaryApi.ORGANIZATION_ID) organizationId: UUID,
        @PathVariable(TimeUseDiaryApi.STUDY_ID) studyId: UUID,
        @PathVariable(TimeUseDiaryApi.PARTICIPANT_ID) participantId: String,
        @RequestParam(TimeUseDiaryApi.DATA_TYPE) type: TimeUseDiaryDownloadDataType,
        @RequestBody submissionIds: Set<UUID>
    ) {
        TODO("Not yet implemented")
    }

    @Timed
    @GetMapping(
        path = [TimeUseDiaryApi.STATUS_PATH]
    )
    override fun isRunning(): Boolean {
        logger.info("TimeUseDiaryAPI is running...")
        return true
    }
}