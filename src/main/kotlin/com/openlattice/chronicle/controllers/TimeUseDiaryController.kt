package com.openlattice.chronicle.controllers

import com.codahale.metrics.annotation.Timed
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
import com.openlattice.chronicle.services.studies.StudyService
import com.openlattice.chronicle.services.timeusediary.TimeUseDiaryService
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi.Companion.CONTROLLER
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi.Companion.DOWNLOAD_TYPE
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi.Companion.END_DATE
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi.Companion.IDS_PATH
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi.Companion.ORGANIZATION_ID_PATH
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi.Companion.PARTICIPANT_ID_PATH
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi.Companion.START_DATE
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi.Companion.STATUS_PATH
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi.Companion.STUDY_ID
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi.Companion.STUDY_ID_PATH
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi.Companion.STUDY_PATH
import com.openlattice.chronicle.timeusediary.TimeUseDiaryDownloadDataType
import com.openlattice.chronicle.timeusediary.TimeUseDiaryResponse
import org.slf4j.LoggerFactory
import org.springframework.format.annotation.DateTimeFormat
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.EnumSet
import java.util.UUID
import javax.servlet.http.HttpServletResponse

/**
 * @author Andrew Carter andrew@openlattice.com
 */

@RestController
@RequestMapping(CONTROLLER)
class TimeUseDiaryController(
    override val authorizationManager: AuthorizationManager,
    override val auditingManager: AuditingManager,
    val storageResolver: StorageResolver,
    val idGenerationService: HazelcastIdGenerationService,
    val timeUseDiaryService: TimeUseDiaryService,
    val studyService: StudyService
) : TimeUseDiaryApi, AuthorizingComponent {

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
        val realStudyId = studyService.getStudyId(studyId)
        checkNotNull(realStudyId) { "invalid study id" }
        return storageResolver.getPlatformStorage().connection.use { conn ->
            AuditedOperationBuilder<UUID>(conn, auditingManager)
                .operation { connection ->
                    timeUseDiaryService.submitTimeUseDiary(
                        connection,
                        organizationId,
                        realStudyId,
                        participantId,
                        responses
                    )
                }
                .audit {
                    listOf(
                        // TODO - fix Principals.getAnonymousSecurablePrincipal(), always throws NPE
                        // AuditableEvent(
                        //     aclKey = AclKey(realStudyId),
                        //     study = realStudyId,
                        //     eventType = AuditEventType.SUBMIT_TIME_USE_DIARY,
                        //     principal = Principals.getAnonymousUser(),
                        //     securablePrincipalId = Principals.getAnonymousSecurablePrincipal().id
                        // )
                    )
                }
                .buildAndRun()
        }
    }

    @Timed
    @GetMapping(
        path = [IDS_PATH + ORGANIZATION_ID_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getParticipantTUDSubmissionsByDate(
        @PathVariable(ORGANIZATION_ID) organizationId: UUID,
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        @RequestParam(START_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) startDateTime: OffsetDateTime,
        @RequestParam(END_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) endDateTime: OffsetDateTime,
    ): Map<LocalDate, Set<UUID>> {
        accessCheck(AclKey(studyId), EnumSet.of(Permission.READ))
        logger.info("Retrieving TimeUseDiary ids from study $studyId for $participantId")
        val submissionsIdsByDate = timeUseDiaryService.getParticipantTUDSubmissionsByDate(
            organizationId,
            studyId,
            participantId,
            startDateTime,
            endDateTime
        )
        recordEvent(
            AuditableEvent(
                AclKey(studyId),
                Principals.getCurrentSecurablePrincipal().id,
                Principals.getCurrentUser(),
                AuditEventType.GET_TIME_USE_DIARY_SUBMISSION,
                "[$participantId]: $startDateTime - $endDateTime",
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

    @Timed
    @GetMapping(
        path = [STUDY_PATH + STUDY_ID_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getStudyTUDSubmissionsByDate(
        @PathVariable(STUDY_ID) studyId: UUID,
        @RequestParam(START_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) startDateTime: OffsetDateTime,
        @RequestParam(END_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) endDateTime: OffsetDateTime,
    ): Map<LocalDate, Set<UUID>> {
        ensureReadAccess(AclKey(studyId))
        logger.info("Retrieving TimeUseDiary ids from study $studyId")
        val submissionsIdsByDate = timeUseDiaryService.getStudyTUDSubmissionsByDate(
            studyId,
            startDateTime,
            endDateTime
        )
        recordEvent(
            AuditableEvent(
                AclKey(studyId),
                eventType = AuditEventType.GET_TIME_USE_DIARY_SUBMISSION,
                description = "$startDateTime - $endDateTime",
                study = studyId,
            )
        )
        return submissionsIdsByDate
    }

    @Override
    override fun downloadTimeUseDiaryData(
        @PathVariable(ORGANIZATION_ID) organizationId: UUID,
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        @RequestParam(DOWNLOAD_TYPE) downloadType: TimeUseDiaryDownloadDataType,
        @RequestBody submissionIds: Set<UUID>
    ): Iterable<Map<String,Any>> {
        throw TimeUseDiaryDownloadExcpetion(studyId,
            "This implementation of the overloaded downloadTimeUseDiaryData method should not be used. " +
                    "Instead use the implementation that includes an HttpServletResponse parameter.")
    }

    @Timed
    @GetMapping(
        path = [ORGANIZATION_ID_PATH + STUDY_ID_PATH + PARTICIPANT_ID_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE]
    )
    fun downloadTimeUseDiaryData(
        @PathVariable(ORGANIZATION_ID) organizationId: UUID,
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        @RequestParam(DOWNLOAD_TYPE) downloadType: TimeUseDiaryDownloadDataType,
        @RequestBody submissionIds: Set<UUID>,
        response: HttpServletResponse
    ): Iterable<Map<String,Any>> {
        accessCheck(AclKey(studyId), EnumSet.of(Permission.READ))
        val data = timeUseDiaryService.downloadTimeUseDiaryData(
            organizationId,
            studyId,
            participantId,
            downloadType,
            submissionIds
        )

        val filename = "$participantId-$downloadType-${LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE)}.csv"
        response.contentType = CustomMediaType.TEXT_CSV_VALUE
        response.setHeader("Content-Disposition","attachment; filename=$filename")

        recordEvent(
            AuditableEvent(
                AclKey(studyId),
                Principals.getCurrentSecurablePrincipal().id,
                Principals.getCurrentUser(),
                AuditEventType.DOWNLOAD_TIME_USE_DIARY_SUBMISSIONS,
                downloadType.toString(),
                studyId,
                organizationId,
                mapOf()
            )
        )

        return data
    }

    @Timed
    @GetMapping(
        path = [STATUS_PATH]
    )
    override fun isRunning(): Boolean {
        logger.info("Time Use Diary API is running...")
        return true
    }
}
