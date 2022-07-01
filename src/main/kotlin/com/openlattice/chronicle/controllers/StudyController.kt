package com.openlattice.chronicle.controllers

import com.codahale.metrics.annotation.Timed
import com.geekbeast.controllers.exceptions.ForbiddenException
import com.google.common.base.MoreObjects
import com.hazelcast.core.HazelcastInstance
import com.openlattice.chronicle.android.ChronicleData
import com.openlattice.chronicle.android.ChronicleUsageEvent
import com.openlattice.chronicle.auditing.AuditEventType
import com.openlattice.chronicle.auditing.AuditableEvent
import com.openlattice.chronicle.auditing.AuditedOperationBuilder
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.*
import com.openlattice.chronicle.authorization.principals.Principals
import com.openlattice.chronicle.base.OK
import com.openlattice.chronicle.base.OK.Companion.ok
import com.openlattice.chronicle.data.FileType
import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.deletion.*
import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.ids.IdConstants
import com.openlattice.chronicle.organizations.ChronicleDataCollectionSettings
import com.openlattice.chronicle.participants.Participant
import com.openlattice.chronicle.participants.ParticipantStats
import com.openlattice.chronicle.sensorkit.SensorDataSample
import com.openlattice.chronicle.sensorkit.SensorSetting
import com.openlattice.chronicle.sensorkit.SensorType
import com.openlattice.chronicle.services.download.DataDownloadService
import com.openlattice.chronicle.services.enrollment.EnrollmentService
import com.openlattice.chronicle.services.jobs.ChronicleJob
import com.openlattice.chronicle.services.jobs.JobService
import com.openlattice.chronicle.services.studies.StudyService
import com.openlattice.chronicle.services.upload.AppDataUploadService
import com.openlattice.chronicle.services.upload.SensorDataUploadService
import com.openlattice.chronicle.sources.SourceDevice
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.study.*
import com.openlattice.chronicle.study.StudyApi.Companion.ANDROID_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.CONTROLLER
import com.openlattice.chronicle.study.StudyApi.Companion.DATA_COLLECTION
import com.openlattice.chronicle.study.StudyApi.Companion.DATA_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.DATA_TYPE
import com.openlattice.chronicle.study.StudyApi.Companion.END_DATE
import com.openlattice.chronicle.study.StudyApi.Companion.ENROLL_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.FILE_NAME
import com.openlattice.chronicle.study.StudyApi.Companion.IOS_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.study.StudyApi.Companion.ORGANIZATION_ID_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.ORGANIZATION_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.PARTICIPANTS_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.study.StudyApi.Companion.PARTICIPANT_ID_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.PARTICIPANT_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.PARTICIPATION_STATUS
import com.openlattice.chronicle.study.StudyApi.Companion.RETRIEVE
import com.openlattice.chronicle.study.StudyApi.Companion.SENSORS_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.SETTINGS_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.SOURCE_DEVICE_ID
import com.openlattice.chronicle.study.StudyApi.Companion.SOURCE_DEVICE_ID_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.START_DATE
import com.openlattice.chronicle.study.StudyApi.Companion.STATS_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.STATUS_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.STUDY_ID
import com.openlattice.chronicle.study.StudyApi.Companion.STUDY_ID_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.VERIFY_PATH
import com.openlattice.chronicle.util.ChronicleServerUtil
import org.slf4j.LoggerFactory
import org.springframework.format.annotation.DateTimeFormat
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import javax.inject.Inject
import javax.servlet.http.HttpServletResponse
import javax.validation.constraints.Size


/**
 * @author Solomon Tang <solomon@openlattice.com>
 */

@RestController
@RequestMapping(CONTROLLER)
class StudyController @Inject constructor(
    hazelcastInstance: HazelcastInstance,
    val storageResolver: StorageResolver,
    val idGenerationService: HazelcastIdGenerationService,
    val enrollmentService: EnrollmentService,
    val studyService: StudyService,
    val sensorDataUploadService: SensorDataUploadService,
    val appDataUploadService: AppDataUploadService,
    val downloadService: DataDownloadService,
    override val authorizationManager: AuthorizationManager,
    override val auditingManager: AuditingManager,
    val chronicleJobService: JobService,
//    private val managementApi: ManagementAPI,
) : StudyApi, AuthorizingComponent {

    private val studies = HazelcastMap.STUDIES.getMap(hazelcastInstance)

    companion object {
        private val logger = LoggerFactory.getLogger(StudyController::class.java)!!
    }

    @Timed
    @PostMapping(
        path = [STUDY_ID_PATH + PARTICIPANT_PATH + PARTICIPANT_ID_PATH + SOURCE_DEVICE_ID_PATH + ENROLL_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE],
    )
    override fun enroll(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        @PathVariable(SOURCE_DEVICE_ID) datasourceId: String,
        @RequestBody sourceDevice: SourceDevice,
    ): UUID {
        val realStudyId = studyService.getStudyId(studyId)
        checkNotNull(realStudyId) { "invalid study id" }
        return enrollmentService.registerDatasource(realStudyId, participantId, datasourceId, sourceDevice)
    }

    @Timed
    @PostMapping(
        path = ["", "/"],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE],
    )
    override fun createStudy(@RequestBody study: Study): UUID {
        if (study.settings.containsKey(StudySettingType.Sensor) && !isAdmin()) {
            throw ForbiddenException("Only admins can modify sensor types.")
        }

        ensureAuthenticated()
        study.organizationIds.forEach { organizationId -> ensureOwnerAccess(AclKey(organizationId)) }
        logger.info("Creating study associated with organizations ${study.organizationIds}")
        return studyService.createStudy(study)
    }

    @Timed
    @GetMapping(
        path = [STUDY_ID_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE],
    )
    override fun getStudy(@PathVariable(STUDY_ID) studyId: UUID): Study {
        ensureReadAccess(AclKey(studyId))
        logger.info("Retrieving study with id $studyId")

        return try {
            val study = studyService.getStudy(studyId)
            recordEvent(
                AuditableEvent(
                    AclKey(studyId),
                    eventType = AuditEventType.GET_STUDY,
                    description = "",
                    study = studyId,
                    organization = IdConstants.UNINITIALIZED.id,
                    data = mapOf()
                )
            )
            study
        } catch (ex: NoSuchElementException) {
            throw StudyNotFoundException(studyId, "No study with id $studyId found.")
        }

    }

    @Timed
    @GetMapping(
        path = [ORGANIZATION_PATH + ORGANIZATION_ID_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE],
    )
    override fun getOrgStudies(@PathVariable(ORGANIZATION_ID) organizationId: UUID): List<Study> {

        ensureReadAccess(AclKey(organizationId))
        val currentUser = Principals.getCurrentSecurablePrincipal()
        logger.info("Retrieving studies with organization id $organizationId on behalf of ${currentUser.principal.id}")

        return try {
            val studies = studyService.getOrgStudies(organizationId)
            studies.forEach { study ->
                recordEvent(
                    AuditableEvent(
                        AclKey(study.id),
                        currentUser.id,
                        currentUser.principal,
                        eventType = AuditEventType.GET_STUDY,
                        study = study.id,
                        organization = organizationId,
                    )
                )
            }

            studies
        } catch (ex: NoSuchElementException) {
            throw OrganizationNotFoundException(organizationId, "No organization with id $organizationId found.")
        }

    }

    @Timed
    @PutMapping(
        path = [STUDY_ID_PATH + SETTINGS_PATH + SENSORS_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun updateStudyAppleSettings(
        @PathVariable(STUDY_ID) studyId: UUID,
        @RequestBody sensorSetting: SensorSetting
    ): OK {
        ensureAdminAccess()

        //We don't need to resolve real study id as this won't ever be called from legacy clients.
        val study = studyService.getStudy(studyId)
        val studySettings = study.settings.toMutableMap()
        studySettings[ StudySettingType.Sensor ] = sensorSetting
        updateStudy(studyId, StudyUpdate(settings = StudySettings(studySettings)))

        return ok
    }

    @Timed
    @PatchMapping(
        path = [STUDY_ID_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun updateStudy(
        @PathVariable(STUDY_ID) studyId: UUID,
        @RequestBody study: StudyUpdate,
        @RequestParam(value = RETRIEVE, required = false, defaultValue = "false") retrieve: Boolean,
    ): Study? {
        if (study.settings?.containsKey(StudySettingType.Sensor) == true && !isAdmin()) {
            throw ForbiddenException("Only admins can modify sensor types.")
        }

        val studyAclKey = AclKey(studyId)
        ensureOwnerAccess(studyAclKey)
        val currentUser = Principals.getCurrentSecurablePrincipal()
        logger.info("Updating study with id $studyId on behalf of ${currentUser.principal.id}")
        storageResolver.getPlatformStorage().connection.use { conn ->
            AuditedOperationBuilder<Unit>(conn, auditingManager)
                .operation { connection -> studyService.updateStudy(connection, studyId, study) }
                .audit {
                    listOf(
                        AuditableEvent(
                            studyAclKey,
                            currentUser.id,
                            currentUser.principal,
                            AuditEventType.UPDATE_STUDY,
                            study = studyId,
                            data = mapOf()
                        )
                    )
                }
                .buildAndRun()
        }
        studyService.refreshStudyCache(setOf(studyId))
        return if (retrieve) studyService.getStudy(studyId) else null
    }

    @Timed
    @DeleteMapping(
        path = [STUDY_ID_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE],
    )
    override fun destroyStudy(@PathVariable studyId: UUID): Iterable<UUID> {
        ensureOwnerAccess(AclKey(studyId))
        val currentUser = Principals.getCurrentSecurablePrincipal()
        logger.info("Deleting study with id $studyId")
        // val currentUserEmail = getUser(managementApi, Principals.getCurrentUser().id).email
        val deleteStudyDataJob = ChronicleJob(
            id = idGenerationService.getNextId(),
            contact = "test@openlattice.com",
            definition = DeleteStudyUsageData(studyId)
        )
        val deleteStudyTUDSubmissionJob = ChronicleJob(
            id = idGenerationService.getNextId(),
            contact = "test@openlattice.com",
            definition = DeleteStudyTUDSubmissionData(studyId)
        )
        val deleteStudyAppUsageSurveyJob = ChronicleJob(
            id = idGenerationService.getNextId(),
            contact = "test@openlattice.com",
            definition = DeleteStudyAppUsageSurveyData(studyId)
        )
        val jobList = listOf(
            deleteStudyDataJob,
            deleteStudyTUDSubmissionJob,
            deleteStudyAppUsageSurveyJob
        )
        return storageResolver.getPlatformStorage().connection.use { conn ->
            AuditedOperationBuilder<Iterable<UUID>>(conn, auditingManager)
                .operation { connection ->
                    val newJobIds = chronicleJobService.createJobs(connection, jobList)
                    logger.info("Created jobs with ids = {}", newJobIds)
                    val studyIdList = listOf(studyId)
                    studyService.deleteStudies(connection, studyIdList)
                    studyService.removeStudiesFromOrganizations(connection, studyIdList)
                    studyService.removeAllParticipantsFromStudies(connection, studyIdList)
                    studies.evict(studyId)
                    return@operation newJobIds
                }
                .audit { jobIds ->
                    listOf(
                        AuditableEvent(
                            AclKey(studyId),
                            currentUser.id,
                            currentUser.principal,
                            AuditEventType.DELETE_STUDY,
                            "",
                            studyId,
                            UUID(0, 0),
                            mapOf()
                        )
                    ) + jobIds.map {
                        AuditableEvent(
                            AclKey(it),
                            currentUser.id,
                            currentUser.principal,
                            AuditEventType.CREATE_JOB,
                            "",
                            studyId
                        )
                    }
                }
                .buildAndRun()
        }
    }

    @Timed
    @DeleteMapping(
        path = [STUDY_ID_PATH + PARTICIPANTS_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE],
    )
    override fun deleteStudyParticipants(
        @PathVariable(STUDY_ID) studyId: UUID,
        @RequestBody participantIds: Set<String>,
    ): Iterable<UUID> {
        ensureValidStudy(studyId)
        ensureWriteAccess(AclKey(studyId))

        val deleteParticipantUsageDataJob = ChronicleJob(
            id = idGenerationService.getNextId(),
            contact = "test@openlattice.com",
            definition = DeleteParticipantUsageData(studyId, participantIds)
        )
        val deleteParticipantTUDSubmissionsJob = ChronicleJob(
            id = idGenerationService.getNextId(),
            contact = "test@openlattice.com",
            definition = DeleteParticipantTUDSubmissionData(studyId, participantIds)
        )
        val deleteParticipantAppUsageSurveysJob = ChronicleJob(
            id = idGenerationService.getNextId(),
            contact = "test@openlattice.com",
            definition = DeleteParticipantAppUsageSurveyData(studyId, participantIds)
        )

        val jobList = listOf(
            deleteParticipantUsageDataJob,
            deleteParticipantTUDSubmissionsJob,
            deleteParticipantAppUsageSurveysJob,
        )

        return storageResolver.getPlatformStorage().connection.use { conn ->
            AuditedOperationBuilder<Iterable<UUID>>(conn, auditingManager)
                .operation { connection ->
                    val newJobIds = chronicleJobService.createJobs(connection, jobList)
                    logger.info("Created jobs with ids = {}", newJobIds)
                    studyService.removeParticipantsFromStudy(connection, studyId, participantIds)
                    return@operation newJobIds
                }
                .audit { jobIds ->
                    listOf(
                        AuditableEvent(
                            AclKey(studyId),
                            eventType = AuditEventType.DELETE_PARTICIPANTS,
                            description = "Participants $participantIds were removed from study $studyId",
                            study = studyId
                        )
                    ) + jobIds.map {
                        AuditableEvent(
                            AclKey(it),
                            eventType = AuditEventType.CREATE_JOB,
                            study = studyId
                        )
                    }
                }
                .buildAndRun()
        }
    }

    @Timed
    @PostMapping(
        path = [STUDY_ID_PATH + PARTICIPANT_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun registerParticipant(
        @PathVariable(STUDY_ID) studyId: UUID,
        @RequestBody participant: Participant,
    ): UUID {
        ensureValidStudy(studyId)
        ensureWriteAccess(AclKey(studyId))

        return studyService.registerParticipant(studyId, participant)
    }

    @Timed
    @PostMapping(
        path = [STUDY_ID_PATH + PARTICIPANT_PATH + PARTICIPANT_ID_PATH + IOS_PATH + SOURCE_DEVICE_ID_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun uploadSensorData(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        @PathVariable(SOURCE_DEVICE_ID) sourceDeviceId: String,
        @RequestBody data: List<SensorDataSample>,
    ): Int {
        return sensorDataUploadService.upload(studyId, participantId, sourceDeviceId, data)
    }

    @Timed
    @PutMapping(
        path = [STUDY_ID_PATH + DATA_COLLECTION],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun setChronicleDataCollectionSettings(
        @PathVariable(STUDY_ID) studyId: UUID,
        @RequestBody dataCollectionSettings: ChronicleDataCollectionSettings,
    ): OK {
        ensureValidStudy(studyId)
        ensureWriteAccess(AclKey(studyId))

        val study = studyService.getStudy(studyId)
        study.settings.toMutableMap()[StudySettingType.DataCollection] = dataCollectionSettings
        storageResolver.getPlatformStorage().connection.use { conn ->
            AuditedOperationBuilder<Unit>(conn, auditingManager)
                .operation { connection ->
                    studyService.updateStudy(
                        connection,
                        studyId,
                        StudyUpdate(settings = study.settings)
                    )
                }
                .audit {
                    listOf(
                        AuditableEvent(
                            aclKey = AclKey(studyId),
                            eventType = AuditEventType.UPDATE_STUDY_SETTINGS
                        )
                    )
                }
                .buildAndRun()
        }
        studies.loadAll(setOf(studyId), true) //Reload updated study into cache
        return OK()
    }

    @PostMapping(
        path = [STUDY_ID_PATH + PARTICIPANT_PATH + PARTICIPANT_ID_PATH + ANDROID_PATH + SOURCE_DEVICE_ID_PATH]
    )
    override fun uploadAndroidUsageEventData(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        @PathVariable(SOURCE_DEVICE_ID) datasourceId: String,
        @RequestBody data: ChronicleData,
    ): Int {
        //TODO: I think we still needs this as long as there is an enrolled participant in a legacy study.
        val realStudyId = studyService.getStudyId(studyId)
        checkNotNull(realStudyId) { "invalid study id" }
        return data.groupBy { it.javaClass }.map { (clazz, dataByClass) ->
            when (clazz) {
                ChronicleUsageEvent::class.java -> appDataUploadService.uploadAndroidUsageEvents(
                    realStudyId,
                    participantId,
                    datasourceId,
                    dataByClass.map { it as ChronicleUsageEvent })
                else -> 0
            }
        }.sum()
    }

    @Timed
    @GetMapping(
        path = [STUDY_ID_PATH + SETTINGS_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getStudySettings(
        @PathVariable(STUDY_ID) studyId: UUID,
    ): Map<StudySettingType, StudySetting> {
        // No permissions check since this is assumed to be invoked from a non-authenticated context
        val realStudyId = studyService.getStudyId(studyId)
        checkNotNull(realStudyId) { "invalid study id" }
        return studyService.getStudySettings(realStudyId)
    }

    @Timed
    @GetMapping(
        path = [STUDY_ID_PATH + SETTINGS_PATH + SENSORS_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getStudySensors(@PathVariable(STUDY_ID) studyId: UUID): Set<SensorType> {
        return studyService.getStudySensors(studyId)
    }

    @Timed
    @GetMapping(
        path = [STUDY_ID_PATH + PARTICIPANTS_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getStudyParticipants(@PathVariable(STUDY_ID) studyId: UUID): Iterable<Participant> {
        ensureAuthenticated()
        ensureReadAccess(AclKey(studyId))
        return studyService.getStudyParticipants(studyId)
    }

    @Timed
    @GetMapping(
        path = ["", "/"],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getAllStudies(): Iterable<Study> {
        ensureAuthenticated()
        val studyAclKeys = authorizationManager.listAuthorizedObjectsOfType(
            Principals.getCurrentPrincipals(),
            SecurableObjectType.Study,
            EnumSet.of(Permission.READ)
        )
        val studies = studyService.getStudies(studyAclKeys.mapTo(mutableSetOf()) { it.first() })

        auditingManager.recordEvents(studies.map {
            AuditableEvent(
                aclKey = AclKey(it.id),
                eventType = AuditEventType.GET_ALL_STUDIES,
                study = it.id,
                description = "Loaded all accessible studies."
            )
        })

        return studies
    }

    @Timed
    @GetMapping(
        path = [STUDY_ID_PATH + PARTICIPANTS_PATH + STATS_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getParticipantStats(@PathVariable(STUDY_ID) studyId: UUID): Map<String, ParticipantStats> {
        ensureReadAccess(AclKey(studyId))
        return studyService.getStudyParticipantStats(studyId)
    }

    override fun getParticipantsData(
        studyId: UUID,
        dataType: ParticipantDataType,
        participantIds: Set<String>,
        startDateTime: OffsetDateTime,
        endDateTime: OffsetDateTime,
    ): Iterable<Map<String, Any>> {
        ensureReadAccess(AclKey(studyId))
        return when (dataType) {
            ParticipantDataType.Preprocessed -> downloadService.getPreprocessedUsageEventsData(
                studyId,
                participantIds,
                startDateTime,
                endDateTime
            )
            ParticipantDataType.AppUsageSurvey -> downloadService.getParticipantsAppUsageSurveyData(
                studyId,
                participantIds,
                startDateTime,
                endDateTime
            )
            ParticipantDataType.IOSSensor -> {
                val sensors = getStudySensors(studyId)
                downloadService.getParticipantsSensorData(studyId, participantIds, sensors, startDateTime, endDateTime)
            }
            ParticipantDataType.UsageEvents -> {
                downloadService.getParticipantsUsageEventsData(studyId, participantIds, startDateTime, endDateTime)
            }
        }
    }

    @PatchMapping(
        path = [STUDY_ID_PATH + PARTICIPANT_PATH + PARTICIPANT_ID_PATH + STATUS_PATH]
    )
    override fun updateParticipationStatus(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        @RequestParam(PARTICIPATION_STATUS) participationStatus: ParticipationStatus,
    ): OK {
        ensureWriteAccess(AclKey(studyId))
        studyService.updateParticipationStatus(studyId, participantId, participationStatus)
        return OK("Successfully updated participation status ${ChronicleServerUtil.STUDY_PARTICIPANT}")
    }

    @Timed
    @GetMapping(
        path = [STUDY_ID_PATH + PARTICIPANTS_PATH + DATA_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    fun getParticipantsData(
        @PathVariable(STUDY_ID) studyId: UUID,
        @RequestParam(value = DATA_TYPE) dataType: ParticipantDataType,
        @RequestParam(value = PARTICIPANT_ID) participantIds: Set<String>,
        @RequestParam(value = START_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) startDateTime: OffsetDateTime?,
        @RequestParam(value = END_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) endDateTime: OffsetDateTime?,
        @RequestParam(value = FILE_NAME) @Size(max = 64) fileName: String?,
        response: HttpServletResponse,
    ): Iterable<Map<String, Any>> {
        val data = getParticipantsData(
            studyId,
            dataType,
            participantIds,
            MoreObjects.firstNonNull(startDateTime, OffsetDateTime.MIN),
            MoreObjects.firstNonNull(endDateTime, OffsetDateTime.MAX)
        )

        ChronicleServerUtil.setDownloadContentType(response, FileType.csv)
        ChronicleServerUtil.setContentDisposition(
            response,
            MoreObjects.firstNonNull(
                fileName,
                "${dataType}_${
                    LocalDate.now()
                        .format(DateTimeFormatter.BASIC_ISO_DATE)
                }"
            ),
            FileType.csv
        )

        recordEvent(
            AuditableEvent(
                aclKey = AclKey(studyId),
                securablePrincipalId = Principals.getCurrentSecurablePrincipal().id,
                principal = Principals.getCurrentUser(),
                eventType = AuditEventType.DOWNLOAD_PARTICIPANTS_DATA,
                description = dataType.toString(),
                study = studyId
            )
        )

        return data
    }

    @Timed
    @GetMapping(
        path = [STUDY_ID_PATH + PARTICIPANT_PATH + PARTICIPANT_ID_PATH + VERIFY_PATH]
    )
    override fun isKnownParticipant(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
    ): Boolean {
        return enrollmentService.isKnownParticipant(studyId, participantId)
    }

    /**
     * Ensures that study id provided is for a valid study.
     *
     */
    private fun ensureValidStudy(studyId: UUID): Boolean {
        return studyService.isValidStudy(studyId)
    }

}
