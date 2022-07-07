package com.openlattice.chronicle.controllers

import com.codahale.metrics.annotation.Timed
import com.google.common.base.MoreObjects
import com.openlattice.chronicle.auditing.AuditEventType
import com.openlattice.chronicle.auditing.AuditableEvent
import com.openlattice.chronicle.auditing.AuditedTransactionBuilder
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.AuthorizingComponent
import com.openlattice.chronicle.authorization.Permission
import com.openlattice.chronicle.authorization.principals.Principals
import com.openlattice.chronicle.data.FileType
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.services.studies.StudyService
import com.openlattice.chronicle.services.timeusediary.TimeUseDiaryService
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi.Companion.CONTROLLER
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi.Companion.DATA_PATH
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi.Companion.DATA_TYPE
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi.Companion.END_DATE
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi.Companion.FILE_NAME
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi.Companion.IDS_PATH
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi.Companion.PARTICIPANTS_PATH
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi.Companion.PARTICIPANT_ID_PATH
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi.Companion.PARTICIPANT_PATH
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi.Companion.START_DATE
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi.Companion.STUDY_ID
import com.openlattice.chronicle.timeusediary.TimeUseDiaryApi.Companion.STUDY_ID_PATH
import com.openlattice.chronicle.timeusediary.TimeUseDiaryDownloadDataType
import com.openlattice.chronicle.timeusediary.TimeUseDiaryResponse
import com.openlattice.chronicle.util.ChronicleServerUtil
import org.slf4j.LoggerFactory
import org.springframework.format.annotation.DateTimeFormat
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.*
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
        path = [STUDY_ID_PATH + PARTICIPANT_PATH + PARTICIPANT_ID_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun submitTimeUseDiary(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        @RequestBody responses: List<TimeUseDiaryResponse>
    ): UUID {
        val realStudyId = studyService.getStudyId(studyId)
        checkNotNull(realStudyId) { "invalid study id" }
        return storageResolver.getPlatformStorage().connection.use { conn ->
            AuditedTransactionBuilder<UUID>(conn, auditingManager)
                .transaction { connection ->
                    timeUseDiaryService.submitTimeUseDiary(
                        connection,
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
        path = [STUDY_ID_PATH + PARTICIPANT_PATH + PARTICIPANT_ID_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getParticipantTUDSubmissionIdsByDate(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        @RequestParam(START_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) startDateTime: OffsetDateTime,
        @RequestParam(END_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) endDateTime: OffsetDateTime,
    ): Map<OffsetDateTime, Set<UUID>> {
        accessCheck(AclKey(studyId), EnumSet.of(Permission.READ))
        logger.info("Retrieving TimeUseDiary ids from study $studyId for $participantId")
        val submissionsIdsByDate = timeUseDiaryService.getParticipantTUDSubmissionsByDate(
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
            )
        )
        submissionsIdsByDate.values.flatten().forEach {
            accessCheck(AclKey(it), EnumSet.of(Permission.READ))
        }
        return submissionsIdsByDate
    }

    @Timed
    @GetMapping(
        path = [STUDY_ID_PATH + IDS_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getStudyTUDSubmissionIdsByDate(
        @PathVariable(STUDY_ID) studyId: UUID,
        @RequestParam(START_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) startDateTime: OffsetDateTime,
        @RequestParam(END_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) endDateTime: OffsetDateTime,
    ): Map<LocalDate, Set<UUID>> {
        ensureReadAccess(AclKey(studyId))
        logger.info("Retrieving TimeUseDiary ids from study $studyId")
        val submissionsIdsByDate = timeUseDiaryService.getStudyTUDSubmissionIdsByDate(
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
    override fun getStudyTUDSubmissions(
        @PathVariable(STUDY_ID) studyId: UUID,
        @RequestParam(DATA_TYPE) dataType: TimeUseDiaryDownloadDataType,
        @RequestParam(START_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) startDateTime: OffsetDateTime,
        @RequestParam(END_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) endDateTime: OffsetDateTime,
    ): Iterable<List<Map<String, Any>>> {
        ensureReadAccess(AclKey(studyId))
        return timeUseDiaryService.getStudyTUDSubmissions(
            studyId,
            participantIds = null,
            dataType,
            startDateTime,
            endDateTime
        )
    }

    @Timed
    @GetMapping(
        path = [STUDY_ID_PATH + DATA_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    fun getStudyTUDSubmissions(
        @PathVariable(STUDY_ID) studyId: UUID,
        @RequestParam(DATA_TYPE) dataType: TimeUseDiaryDownloadDataType,
        @RequestParam(START_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) startDateTime: OffsetDateTime,
        @RequestParam(END_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) endDateTime: OffsetDateTime,
        response: HttpServletResponse
    ): Iterable<List<Map<String, Any>>> {
        val data = getStudyTUDSubmissions(
            studyId,
            dataType,
            startDateTime,
            endDateTime
        )

        val filename = "TimeUseDiary_${dataType}_${LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE)}.csv"

        ChronicleServerUtil.setDownloadContentType(response, FileType.csv)
        ChronicleServerUtil.setContentDisposition(response, filename, FileType.csv)

        recordEvent(
            AuditableEvent(
                aclKey = AclKey(studyId),
                securablePrincipalId = Principals.getCurrentSecurablePrincipal().id,
                principal = Principals.getCurrentUser(),
                eventType = AuditEventType.DOWNLOAD_TIME_USE_DIARY_DATA,
                description = dataType.toString(),
                study = studyId
            )
        )

        return data
    }

    override fun getParticipantsTudSubmissions(
        studyId: UUID,
        participantIds: Set<String>,
        dataType: TimeUseDiaryDownloadDataType,
        startDateTime: OffsetDateTime,
        endDateTime: OffsetDateTime
    ): Iterable<List<Map<String, Any>>> {
        ensureReadAccess(AclKey(studyId))
        return timeUseDiaryService.getStudyTUDSubmissions(
            studyId,
            participantIds,
            dataType,
            startDateTime,
            endDateTime
        )
    }

    @Timed
    @GetMapping(
        path = [STUDY_ID_PATH + PARTICIPANTS_PATH + DATA_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    fun getParticipantsTudSubmissions(
        @PathVariable(STUDY_ID) studyId: UUID,
        @RequestParam(PARTICIPANT_ID) participantIds: Set<String>,
        @RequestParam(DATA_TYPE) dataType: TimeUseDiaryDownloadDataType,
        @RequestParam(START_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) startDateTime: OffsetDateTime?,
        @RequestParam(END_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) endDateTime: OffsetDateTime?,
        @RequestParam(FILE_NAME) fileName: String?,
        response: HttpServletResponse
    ): Iterable<List<Map<String, Any>>> {
        val data = getParticipantsTudSubmissions(
            studyId,
            participantIds,
            dataType,
            MoreObjects.firstNonNull(startDateTime, OffsetDateTime.MIN),
            MoreObjects.firstNonNull(endDateTime, OffsetDateTime.MAX)
        )

        ChronicleServerUtil.setDownloadContentType(response, FileType.csv)
        ChronicleServerUtil.setContentDisposition(
            response,
            MoreObjects.firstNonNull(fileName, "TimeUseDiary_${dataType}_${LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE)}"), FileType.csv)

        recordEvent(
            AuditableEvent(
                aclKey = AclKey(studyId),
                securablePrincipalId = Principals.getCurrentSecurablePrincipal().id,
                principal = Principals.getCurrentUser(),
                eventType = AuditEventType.DOWNLOAD_PARTICIPANTS_TIME_USE_DIARY_DATA,
                description = "type = $dataType, participants = $participantIds",
                study = studyId
            )
        )

        return data
    }
}
