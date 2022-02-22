package com.openlattice.chronicle.controllers

import com.codahale.metrics.annotation.Timed
import com.geekbeast.configuration.postgres.PostgresFlavor
import com.google.common.collect.SetMultimap
import com.openlattice.chronicle.auditing.AuditEventType
import com.openlattice.chronicle.auditing.AuditableEvent
import com.openlattice.chronicle.auditing.AuditedOperationBuilder
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.*
import com.openlattice.chronicle.authorization.principals.Principals
import com.openlattice.chronicle.base.OK
import com.openlattice.chronicle.data.FileType
import com.openlattice.chronicle.deletion.DeleteStudyAppUsageSurveyData
import com.openlattice.chronicle.deletion.DeleteStudyTUDSubmissionData
import com.openlattice.chronicle.deletion.DeleteStudyUsageData
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.ids.IdConstants
import com.openlattice.chronicle.jobs.ChronicleJob
import com.openlattice.chronicle.organizations.ChronicleDataCollectionSettings
import com.openlattice.chronicle.participants.Participant
import com.openlattice.chronicle.sensorkit.SensorDataSample
import com.openlattice.chronicle.sensorkit.SensorType
import com.openlattice.chronicle.services.download.DataDownloadService
import com.openlattice.chronicle.services.enrollment.EnrollmentService
import com.openlattice.chronicle.services.jobs.JobService
import com.openlattice.chronicle.services.legacy.LegacyUtil
import com.openlattice.chronicle.services.studies.StudyService
import com.openlattice.chronicle.services.upload.AppDataUploadService
import com.openlattice.chronicle.services.upload.SensorDataUploadService
import com.openlattice.chronicle.sources.SourceDevice
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.study.Study
import com.openlattice.chronicle.study.StudyApi
import com.openlattice.chronicle.study.StudyApi.Companion.ANDROID_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.CONTROLLER
import com.openlattice.chronicle.study.StudyApi.Companion.DATA_COLLECTION
import com.openlattice.chronicle.study.StudyApi.Companion.DATA_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.ENROLL_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.IOS_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.study.StudyApi.Companion.ORGANIZATION_ID_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.ORGANIZATION_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.PARTICIPANTS_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.study.StudyApi.Companion.PARTICIPANT_ID_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.PARTICIPANT_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.RETRIEVE
import com.openlattice.chronicle.study.StudyApi.Companion.SENSORS_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.SETTINGS_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.SOURCE_DEVICE_ID
import com.openlattice.chronicle.study.StudyApi.Companion.SOURCE_DEVICE_ID_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.STUDY_ID
import com.openlattice.chronicle.study.StudyApi.Companion.STUDY_ID_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.VERIFY_PATH
import com.openlattice.chronicle.study.StudyUpdate
import com.openlattice.chronicle.util.ChronicleServerUtil
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PatchMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.util.*
import javax.inject.Inject
import javax.servlet.http.HttpServletResponse
import kotlin.NoSuchElementException
import kotlin.streams.asSequence


/**
 * @author Solomon Tang <solomon@openlattice.com>
 */

@RestController
@RequestMapping(CONTROLLER)
class StudyController @Inject constructor(
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
        @RequestBody sourceDevice: SourceDevice
    ): UUID {
//        check( enrollmentService.isKnownParticipant(studyId, participantId)) { "Cannot enroll device for an unknown participant." }
//        TODO: Move checks out from enrollment data source into the controller.
        val deviceId = enrollmentService.registerDatasource(studyId, participantId, datasourceId, sourceDevice)
        val organizationIds = studyService.getStudy(studyId).organizationIds

        /**
         * We don't record an enrollment event into each organization as the organization associated with a study
         * can change.
         */
        recordEvent(
            AuditableEvent(
                AclKey(deviceId),
                eventType = AuditEventType.ENROLL_DEVICE,
                description = "Enrolled ${sourceDevice.javaClass}",
                study = studyId,
                organization = IdConstants.UNINITIALIZED.id,
                data = mapOf("device" to sourceDevice)
            )
        )

        return deviceId
    }

    @Timed
    @PostMapping(
        path = ["", "/"],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE],
    )
    override fun createStudy(@RequestBody study: Study): UUID {
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
    @PatchMapping(
        path = [STUDY_ID_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun updateStudy(
        @PathVariable(STUDY_ID) studyId: UUID,
        @RequestBody study: StudyUpdate,
        @RequestParam(value = RETRIEVE, required = false, defaultValue = "false") retrieve: Boolean
    ): Study? {
        val studyAclKey = AclKey(studyId);
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
    @PostMapping(
        path = [STUDY_ID_PATH + PARTICIPANT_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun registerParticipant(
        @PathVariable(STUDY_ID) studyId: UUID,
        @RequestBody participant: Participant
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
        @RequestBody dataCollectionSettings: ChronicleDataCollectionSettings
    ): OK {
        ensureValidStudy(studyId)
        ensureWriteAccess(AclKey(studyId))

        val study = studyService.getStudy(studyId)
        study.settings.toMutableMap()[LegacyUtil.DATA_COLLECTION] = dataCollectionSettings
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

        return OK()
    }

    @PostMapping(
        path = [STUDY_ID_PATH + PARTICIPANT_PATH + PARTICIPANT_ID_PATH + ANDROID_PATH + SOURCE_DEVICE_ID_PATH]
    )
    override fun uploadAndroidUsageEventData(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        @PathVariable(SOURCE_DEVICE_ID) datasourceId: String,
        @RequestBody data: List<SetMultimap<UUID, Any>>
    ): Int {
        return appDataUploadService.upload(studyId, participantId, datasourceId, data)
    }

    @Timed
    @GetMapping(
        path = [STUDY_ID_PATH + SETTINGS_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getStudySettings(
        @PathVariable(STUDY_ID) studyId: UUID
    ): Map<String, Any> {
        // No permissions check since this is assumed to be invoked from a non-authenticated context
        return studyService.getStudySettings(studyId)
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
        path = [STUDY_ID_PATH + PARTICIPANT_PATH + PARTICIPANT_ID_PATH + DATA_PATH + IOS_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    fun downloadSensorData(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        response: HttpServletResponse
    ): Iterable<Map<String, Any>> {

        val study = getStudy(studyId)
        val sensors = study.retrieveConfiguredSensors()

        if (sensors.isEmpty()) {
            logger.warn(
                "study does not have any configured sensors, exiting download" + ChronicleServerUtil.STUDY_PARTICIPANT,
                studyId,
                participantId
            )
            return listOf()
        }

        val data = downloadService.getParticipantSensorData(studyId, participantId, sensors)
        val fileName = ChronicleServerUtil.getSensorDataFileName(participantId)

        ChronicleServerUtil.setContentDisposition(response, fileName, FileType.csv)
        ChronicleServerUtil.setDownloadContentType(response, FileType.csv)

        return data
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
        path = ["","/"],
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
        path = [STUDY_ID_PATH + PARTICIPANT_PATH + PARTICIPANT_ID_PATH + VERIFY_PATH]
    )
    override fun isKnownParticipant(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String
    ): Boolean {
       return enrollmentService.isKnownParticipant(studyId, participantId)
    }

    /**
     * Ensures that study id provided is for a valid study.
     *
     */
    private fun ensureValidStudy(studyId: UUID): Boolean {
        return true
    }

}
