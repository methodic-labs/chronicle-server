package com.openlattice.chronicle.controllers

import com.codahale.metrics.annotation.Timed
import com.geekbeast.configuration.postgres.PostgresFlavor
import com.openlattice.chronicle.api.TimeUseDiaryApi
import com.openlattice.chronicle.api.TimeUseDiaryApi.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.api.TimeUseDiaryApi.Companion.STUDY_ID
import com.openlattice.chronicle.api.TimeUseDiaryApi.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.api.TimeUseDiaryApi.Companion.START_DATE
import com.openlattice.chronicle.api.TimeUseDiaryApi.Companion.END_DATE
import com.openlattice.chronicle.api.TimeUseDiaryApi.Companion.ZONE_OFFSET
import com.openlattice.chronicle.api.TimeUseDiaryApi.Companion.DATA_TYPE
import com.openlattice.chronicle.auditing.AuditEventType
import com.openlattice.chronicle.auditing.AuditableEvent
import com.openlattice.chronicle.auditing.AuditedOperationBuilder
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.AuthorizingComponent
import com.openlattice.chronicle.authorization.Permission
import com.openlattice.chronicle.authorization.principals.Principals
import com.openlattice.chronicle.constants.CustomMediaType
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.services.timeusediary.TimeUseDiaryManager
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.timeusediary.TimeUseDiaryDownloadDataType
import com.openlattice.chronicle.timeusediary.TimeUseDiaryResponse
import org.slf4j.LoggerFactory
import org.springframework.format.annotation.DateTimeFormat
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.*
import javax.inject.Inject
import javax.servlet.http.HttpServletResponse

/**
 * @author Andrew Carter andrew@openlattice.com
 */

@RestController
@RequestMapping(TimeUseDiaryApi.CONTROLLER)
class TimeUseDiaryController(
    override val authorizationManager: AuthorizationManager,
    override val auditingManager: AuditingManager,
    val storageResolver: StorageResolver,
    val idGenerationService: HazelcastIdGenerationService
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
        @PathVariable(ORGANIZATION_ID) organizationId: UUID,
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        @RequestBody responses: List<TimeUseDiaryResponse>
    ): UUID {
        ensureAuthenticated()
        val (flavor, hds) = storageResolver.getPlatformStorage()
        check(flavor == PostgresFlavor.VANILLA)
        val timeUseDiaryId = idGenerationService.getNextId()
        AuditedOperationBuilder<Unit>(hds.connection, auditingManager)
            .operation { connection ->
                timeUseDiaryManager.submitTimeUseDiary(
                    connection,
                    timeUseDiaryId,
                    organizationId,
                    studyId,
                    participantId,
                    responses
                ) }
            .audit { listOf(
                AuditableEvent(
                    AclKey(timeUseDiaryId),
                    Principals.getCurrentSecurablePrincipal().id,
                    Principals.getCurrentUser().id,
                    AuditEventType.SUBMIT_TIME_USE_DIARY,
                    ""
                )
            ) }
            .buildAndRun()
        return timeUseDiaryId
    }

    @Timed
    @GetMapping(
        path = [TimeUseDiaryApi.IDS_PATH + TimeUseDiaryApi.ORGANIZATION_ID_PATH + TimeUseDiaryApi.STUDY_ID_PATH + TimeUseDiaryApi.PARTICIPANT_ID_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getSubmissionsByDate(
        @PathVariable(ORGANIZATION_ID) organizationId: UUID,
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        @RequestParam(START_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) startDateTime: LocalDateTime,
        @RequestParam(END_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) endDateTime: LocalDateTime,
        @RequestParam(value = TimeUseDiaryApi.ZONE_OFFSET, defaultValue = pstOffset) @DateTimeFormat(pattern = "Z") zoneOffset: ZoneOffset
    ): Map<LocalDate, Set<UUID>> {
        accessCheck(AclKey(studyId), EnumSet.of(Permission.READ))
        logger.info("Retrieving TimeUseDiary ids from study $studyId")
        val submissionsIdsByDate = timeUseDiaryManager.getSubmissionByDate(
            organizationId,
            studyId,
            participantId,
            startDateTime,
            endDateTime,
            zoneOffset
        )
        recordEvent(
            AuditableEvent(
                AclKey(studyId),
                Principals.getCurrentSecurablePrincipal().id,
                Principals.getCurrentUser().id,
                AuditEventType.GET_TIME_USE_DIARY_SUBMISSION,
                "$startDateTime - $endDateTime",
                studyId,
                organizationId,
                mapOf()
                )
        )
        submissionsIdsByDate.values.flatten().forEach {
            accessCheck(AclKey(it), EnumSet.of(Permission.READ))
        }
        return submissionsIdsByDate
    }

    @Override
    override fun downloadTimeUseDiaryData(
        @PathVariable(ORGANIZATION_ID) organizationId: UUID,
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        @RequestParam(DATA_TYPE) type: TimeUseDiaryDownloadDataType,
        @RequestBody submissionIds: Set<UUID>
    ): Iterable<Map<String, Set<Any>>> {
        return timeUseDiaryManager.downloadTimeUseDiaryData(
            organizationId,
            studyId,
            participantId,
            type,
            submissionIds
        )
    }

    @Timed
    @GetMapping(
        path = [TimeUseDiaryApi.ORGANIZATION_ID_PATH + TimeUseDiaryApi.STUDY_ID_PATH + TimeUseDiaryApi.PARTICIPANT_ID_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE]
    )
    fun downloadTimeUseDiaryData(
        @PathVariable(ORGANIZATION_ID) organizationId: UUID,
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        @RequestParam(DATA_TYPE) type: TimeUseDiaryDownloadDataType,
        @RequestBody submissionIds: Set<UUID>,
        response: HttpServletResponse
    ): Iterable<Map<String, Set<Any>>> {
        val data = timeUseDiaryManager.downloadTimeUseDiaryData(
            organizationId,
            studyId,
            participantId,
            type,
            submissionIds
        )
        response.contentType = CustomMediaType.TEXT_CSV_VALUE
        response.setHeader("Content-Disposition","attachment; filename=test_file.csv")
        return data
    }

    @Timed
    @GetMapping(
        path = [TimeUseDiaryApi.STATUS_PATH]
    )
    override fun isRunning(): Boolean {
        logger.info("Time Use Diary API is running...")
        return true
    }
}