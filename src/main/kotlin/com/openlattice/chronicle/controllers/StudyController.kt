package com.openlattice.chronicle.controllers

import com.auth0.client.mgmt.ManagementAPI
import com.codahale.metrics.annotation.Timed
import com.geekbeast.configuration.postgres.PostgresFlavor
import com.openlattice.chronicle.auditing.AuditEventType
import com.openlattice.chronicle.auditing.AuditableEvent
import com.openlattice.chronicle.auditing.AuditedOperationBuilder
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.AuthorizingComponent
import com.openlattice.chronicle.authorization.Permission
import com.openlattice.chronicle.authorization.principals.Principals
import com.openlattice.chronicle.data.FileType
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.ids.IdConstants
import com.openlattice.chronicle.jobs.ChronicleJob
import com.openlattice.chronicle.jobs.DeleteStudyUsageData
import com.openlattice.chronicle.participants.Participant
import com.openlattice.chronicle.sensorkit.SensorDataSample
import com.openlattice.chronicle.services.download.DataDownloadService
import com.openlattice.chronicle.services.enrollment.EnrollmentService
import com.openlattice.chronicle.services.jobs.JobService
import com.openlattice.chronicle.services.studies.StudyService
import com.openlattice.chronicle.services.upload.SensorDataUploadService
import com.openlattice.chronicle.sources.SourceDevice
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.study.Study
import com.openlattice.chronicle.study.StudyApi
import com.openlattice.chronicle.study.StudyApi.Companion.CONTROLLER
import com.openlattice.chronicle.study.StudyApi.Companion.DATA_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.DATA_SOURCE_ID
import com.openlattice.chronicle.study.StudyApi.Companion.DATA_SOURCE_ID_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.ENROLL_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.study.StudyApi.Companion.ORGANIZATION_ID_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.ORGANIZATION_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.study.StudyApi.Companion.PARTICIPANT_ID_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.PARTICIPANT_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.RETRIEVE
import com.openlattice.chronicle.study.StudyApi.Companion.SENSOR_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.STUDY_ID
import com.openlattice.chronicle.study.StudyApi.Companion.STUDY_ID_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.UPLOAD_PATH
import com.openlattice.chronicle.study.StudyUpdate
import com.openlattice.chronicle.util.ChronicleServerUtil
import com.openlattice.users.getUser
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PatchMapping
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.util.*
import javax.inject.Inject
import javax.servlet.http.HttpServletResponse


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
    val chronicleJobService: JobService,
    private val managementApi: ManagementAPI,
    val downloadService: DataDownloadService,
    override val authorizationManager: AuthorizationManager,
    override val auditingManager: AuditingManager
) : StudyApi, AuthorizingComponent {


    companion object {
        private val logger = LoggerFactory.getLogger(StudyController::class.java)!!
    }

    @Timed
    @PostMapping(
        path = [STUDY_ID_PATH + PARTICIPANT_PATH + PARTICIPANT_ID_PATH + DATA_SOURCE_ID_PATH + ENROLL_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE],
    )
    override fun enroll(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        @PathVariable(DATA_SOURCE_ID) datasourceId: String,
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
        val (flavor, hds) = storageResolver.getDefaultPlatformStorage()
        check(flavor == PostgresFlavor.VANILLA) { "Only vanilla postgres supported for studies." }
        study.id = idGenerationService.getNextId()

        hds.connection.use { connection ->
            AuditedOperationBuilder<Unit>(connection, auditingManager)
                .operation { connection -> studyService.createStudy(connection, study) }
                .audit {
                    listOf(
                        AuditableEvent(
                            AclKey(study.id),
                            eventType = AuditEventType.CREATE_STUDY,
                            description = "",
                            study = study.id,
                            organization = IdConstants.UNINITIALIZED.id,
                            data = mapOf()
                        )
                    ) + study.organizationIds.map { organizationId ->
                        AuditableEvent(
                            AclKey(study.id),
                            eventType = AuditEventType.ASSOCIATE_STUDY,
                            description = "",
                            study = study.id,
                            organization = organizationId,
                            data = mapOf()
                        )
                    }
                }
                .buildAndRun()
        }
        return study.id
    }

    @Timed
    @GetMapping(
        path = [STUDY_ID_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE],
    )
    override fun getStudy(@PathVariable(STUDY_ID) studyId: UUID): Study {
        accessCheck(AclKey(studyId), EnumSet.of(Permission.READ))
        logger.info("Retrieving study with id $studyId")

        return try {
            val study = studyService.getStudy(studyId)
            recordEvent(
                AuditableEvent(
                    AclKey(studyId),
                    Principals.getCurrentSecurablePrincipal().id,
                    Principals.getCurrentUser().id,
                    AuditEventType.GET_STUDY,
                    "",
                    studyId,
                    IdConstants.UNINITIALIZED.id,
                    mapOf()
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
        val currentUserId = Principals.getCurrentUser().id;
        logger.info("Retrieving studies with organization id $organizationId on behalf of $currentUserId")

        return try {
            val studies = studyService.getOrgStudies(organizationId)
            studies.forEach { study ->
                recordEvent(
                    AuditableEvent(
                        AclKey(study.id),
                        Principals.getCurrentSecurablePrincipal().id,
                        currentUserId,
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
        val currentUserId = Principals.getCurrentUser().id;
        logger.info("Updating study with id $studyId on behalf of $currentUserId")

        val hds = storageResolver.getPlatformStorage()
        hds.connection.use { connection ->
            AuditedOperationBuilder<Unit>(connection, auditingManager)
                .operation { connection -> studyService.updateStudy(connection, studyId, study) }
                .audit {
                    listOf(
                        AuditableEvent(
                            studyAclKey,
                            Principals.getCurrentSecurablePrincipal().id,
                            currentUserId,
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
    override fun destroyStudy(@PathVariable studyId: UUID) {
        accessCheck(AclKey(studyId), EnumSet.of(Permission.OWNER))
        logger.info("Deleting study with id $studyId")

        val hds = storageResolver.getPlatformStorage()
        AuditedOperationBuilder<Unit>(hds.connection, auditingManager)
            .operation { connection -> studyService.deleteStudies(connection, listOf(studyId)) }
            .audit {
                listOf(
                    AuditableEvent(
                        AclKey(studyId),
                        Principals.getCurrentSecurablePrincipal().id,
                        Principals.getCurrentUser().id,
                        AuditEventType.DELETE_STUDY,
                        "",
                        studyId,
                        UUID(0, 0),
                        mapOf()
                    )
                )
            }
            .buildAndRun()

        val currentUserEmail = getUser(managementApi, Principals.getCurrentUser().id).email
        val deleteStudyDataJob = ChronicleJob(
            contact = currentUserEmail,
            jobData = DeleteStudyUsageData(studyId)
        )
        chronicleJobService.createJob(hds.connection, deleteStudyDataJob)

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
        val hds = storageResolver.getPlatformStorage()
        return AuditedOperationBuilder<UUID>(hds.connection, auditingManager)
            .operation { connection -> studyService.registerParticipant(connection, studyId, participant) }
            .audit { candidateId ->
                listOf(
                    AuditableEvent(
                        AclKey(candidateId),
                        eventType = AuditEventType.REGISTER_CANDIDATE,
                        description = "Registering participant with $candidateId for study $studyId."
                    )
                )
            }
            .buildAndRun()
    }

    @Timed
    @PostMapping(
            path = [STUDY_ID_PATH + PARTICIPANT_ID_PATH + DATA_SOURCE_ID_PATH + UPLOAD_PATH + SENSOR_PATH],
            consumes = [MediaType.APPLICATION_JSON_VALUE],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun uploadSensorData(
            @PathVariable(STUDY_ID) studyId: UUID,
            @PathVariable(PARTICIPANT_ID) participantId: String,
            @PathVariable(DATA_SOURCE_ID) datasourceId: String,
            @RequestBody data: List<SensorDataSample>,
    ): Int {
        return sensorDataUploadService.upload(studyId, participantId, datasourceId, data)
    }

    @Timed
    @GetMapping(
        path = [STUDY_ID_PATH + PARTICIPANT_ID_PATH + DATA_PATH + SENSOR_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    fun downloadSensorData(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(PARTICIPANT_ID) participantId: String,
        response: HttpServletResponse
    ): Iterable<Map<String, Any>> {

        val study = getStudy(studyId)
        val sensors = study.getConfiguredSensors()

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

    /**
     * Ensures that study id provided is for a valid study.
     *
     */
    private fun ensureValidStudy(studyId: UUID): Boolean {
        return true
    }

}
