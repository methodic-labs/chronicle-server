package com.openlattice.chronicle.controllers

import com.codahale.metrics.annotation.Timed
import com.geekbeast.controllers.exceptions.ForbiddenException
import com.google.common.base.MoreObjects
import com.hazelcast.core.HazelcastInstance
import com.openlattice.chronicle.android.ChronicleData
import com.openlattice.chronicle.android.ChronicleUsageEvent
import com.openlattice.chronicle.auditing.AuditEventType
import com.openlattice.chronicle.auditing.AuditableEvent
import com.openlattice.chronicle.auditing.AuditedTransactionBuilder
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.*
import com.openlattice.chronicle.authorization.principals.Principals
import com.openlattice.chronicle.base.OK
import com.openlattice.chronicle.base.OK.Companion.ok
import com.openlattice.chronicle.constants.CustomMediaType
import com.openlattice.chronicle.data.FileType
import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.deletion.*
import com.openlattice.chronicle.directory.UserDirectoryService
import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.ids.IdConstants
import com.openlattice.chronicle.organizations.ChronicleDataCollectionSettings
import com.openlattice.chronicle.participants.Participant
import com.openlattice.chronicle.participants.ParticipantStats
import com.openlattice.chronicle.sensorkit.SensorDataSample
import com.openlattice.chronicle.sensorkit.SensorType
import com.openlattice.chronicle.services.download.DataDownloadService
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
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
import com.openlattice.chronicle.study.StudyApi.Companion.PERMISSIONS_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.EMAIL
import com.openlattice.chronicle.study.StudyApi.Companion.RESPONSE_TYPE
import com.openlattice.chronicle.study.StudyApi.Companion.RETRIEVE
import com.openlattice.chronicle.study.StudyApi.Companion.SEARCH_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.SENSORS_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.SETTINGS_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.SETTING_TYPE
import com.openlattice.chronicle.study.StudyApi.Companion.SETTING_TYPE_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.SOURCE_DEVICE_ID
import com.openlattice.chronicle.study.StudyApi.Companion.SOURCE_DEVICE_ID_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.START_DATE
import com.openlattice.chronicle.study.StudyApi.Companion.STATS_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.STATUS_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.STUDY_ID
import com.openlattice.chronicle.study.StudyApi.Companion.STUDY_ID_PATH
import com.openlattice.chronicle.study.StudyApi.Companion.VERIFY_PATH
import com.openlattice.chronicle.users.ChronicleUser
import com.openlattice.chronicle.users.toChronicleUser
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
    val enrollmentManager: EnrollmentManager,
    override val authorizationManager: AuthorizationManager,
    override val auditingManager: AuditingManager,
    val chronicleJobService: JobService,
    val userDirectoryService: UserDirectoryService,
//    private val managementApi: ManagementAPI,
) : StudyApi, AuthorizingComponent {

    private val studies = HazelcastMap.STUDIES.getMap(hazelcastInstance)

    companion object {
        private val logger = LoggerFactory.getLogger(StudyController::class.java)!!

        /**
         * What a study owner -- a study admin -- is granted on the study acl key. Owners can manage the study and
         * manage who else has access to it.
         */
        private val STUDY_OWNER_PERMISSIONS: EnumSet<Permission> = EnumSet.allOf(Permission::class.java)

        /** What someone who can manage a study, but not who has access to it, is granted. */
        private val STUDY_MANAGE_PERMISSIONS: EnumSet<Permission> = EnumSet.of(Permission.READ, Permission.WRITE)

        /** What someone who can only look at a study is granted. */
        private val STUDY_VIEW_PERMISSIONS: EnumSet<Permission> = EnumSet.of(Permission.READ)

        /**
         * The minimum an existing ace must carry to count as owner access. Classification is deliberately looser than
         * [STUDY_OWNER_PERMISSIONS] so that acls written before MATERIALIZE/LINK/INTEGRATE were handed out still read
         * back as owners.
         */
        private val STUDY_OWNER_ACCESS: EnumSet<Permission> =
            EnumSet.of(Permission.READ, Permission.WRITE, Permission.OWNER)
    }

    /**
     * This call needs to be efficient as it is invoked at enrollment and everytime a phone attempts to upload data.
     *
     * TODO: Speed up getStudyId so that it uses in memory cache of legacy studies instead of postgres lookup everytime.
     * Will probably save on aurora bill too. Filed under CHRONICLE-2
     */
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
        val id = enrollmentService.registerDevice(realStudyId, participantId, datasourceId, sourceDevice)
        studyService.updateLastDevicePing(realStudyId, participantId, sourceDevice)
        return id
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
    @PatchMapping(
        path = [STUDY_ID_PATH + SETTINGS_PATH + SETTING_TYPE_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun updateStudySettings(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(SETTING_TYPE) studySetting: StudySettingType,
        @RequestBody setting: StudySetting,
    ): OK {
        when (studySetting) {
            StudySettingType.Sensor -> ensureAdminAccess()
            else -> ensureWriteAccess(AclKey(studyId))
        }

        //We don't need to resolve real study id as this won't ever be called from legacy clients.
        val study = studyService.getStudy(studyId)

        //Make sure we preserve existing settings and only replace the specify study setting.
        val studySettings = study.settings.toMutableMap()
        studySettings[studySetting] = setting
        updateStudy(studyId, StudyUpdate(settings = StudySettings(studySettings)))

        return ok
    }

    @Timed
    @GetMapping(
        path = [STUDY_ID_PATH + PERMISSIONS_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getStudyPermissions(@PathVariable(STUDY_ID) studyId: UUID): StudyPermissions {
        val studyAclKey = AclKey(studyId)
        ensureOwnerAccess(studyAclKey)
        return readStudyPermissions(studyAclKey)
    }

    @Timed
    @GetMapping(
        path = [STUDY_ID_PATH + PERMISSIONS_PATH + SEARCH_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun searchUsersForStudy(
        @PathVariable(STUDY_ID) studyId: UUID,
        @RequestParam(EMAIL) email: String,
    ): List<ChronicleUser> {
        // Owner access is the bar because granting study access is the only reason to browse the directory from here,
        // and only owners may grant it. A too-short query is rejected by the directory itself, as a 400.
        ensureOwnerAccess(AclKey(studyId))

        return userDirectoryService.searchUsersByEmail(email).values
            .map { it.toChronicleUser() }
            .sortedBy { it.email ?: it.principal.id }
    }

    @Timed
    @PostMapping(
        path = [STUDY_ID_PATH + PERMISSIONS_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun updateStudyPermissions(
        @PathVariable(STUDY_ID) studyId: UUID,
        @RequestBody permissionsUpdate: StudyPermissionsUpdate
    ): StudyPermissions {
        val studyAclKey = AclKey(studyId)
        ensureOwnerAccess(studyAclKey)

        ensureStudyRetainsAnOwner(studyId, permissionsUpdate)

        // Revocations run first so that a single request can demote a principal -- revoke the level they hold, grant
        // the level they should hold -- without the grant being undone by the revoke.

        // Revoking view access revokes all access, so it strips every permission, including the ones only an owner
        // would hold.
        permissionsUpdate.revokeViewStudy.forEach {
            authorizationManager.removePermission(studyAclKey, userPrincipal(it), STUDY_OWNER_PERMISSIONS)
        }

        permissionsUpdate.revokeManageStudy.forEach {
            authorizationManager.removePermission(
                studyAclKey,
                userPrincipal(it),
                EnumSet.of(Permission.OWNER, Permission.WRITE)
            )
        }

        permissionsUpdate.revokeOwnerStudy.forEach {
            authorizationManager.removePermission(studyAclKey, userPrincipal(it), EnumSet.of(Permission.OWNER))
        }

        permissionsUpdate.grantViewStudy.forEach {
            authorizationManager.addPermission(studyAclKey, userPrincipal(it), STUDY_VIEW_PERMISSIONS)
        }

        permissionsUpdate.grantManageStudy.forEach {
            authorizationManager.addPermission(studyAclKey, userPrincipal(it), STUDY_MANAGE_PERMISSIONS)
        }

        permissionsUpdate.grantOwnerStudy.forEach {
            authorizationManager.addPermission(studyAclKey, userPrincipal(it), STUDY_OWNER_PERMISSIONS)
        }

        recordStudyPermissionEvents(studyId, permissionsUpdate)

        return readStudyPermissions(studyAclKey)
    }

    /**
     * Buckets a study's live aces by level of access and resolves each principal against the user directory, so that
     * callers get an email address and login type instead of an opaque Auth0 id.
     */
    private fun readStudyPermissions(studyAclKey: AclKey): StudyPermissions {
        val aces = liveAces(studyAclKey)
        val directory = resolveDirectoryEntries(aces.map { it.principal })

        val owners = mutableSetOf<ChronicleUser>()
        val managers = mutableSetOf<ChronicleUser>()
        val viewers = mutableSetOf<ChronicleUser>()

        aces.forEach { ace ->
            val bucket = when {
                ace.permissions.containsAll(STUDY_OWNER_ACCESS) -> owners
                ace.permissions.containsAll(STUDY_MANAGE_PERMISSIONS) -> managers
                ace.permissions.containsAll(STUDY_VIEW_PERMISSIONS) -> viewers
                else -> null
            }
            bucket?.add(directory[ace.principal] ?: ChronicleUser(ace.principal))
        }

        return StudyPermissions(owners, managers, viewers)
    }

    /** A study's aces, minus any whose grant has already expired. */
    private fun liveAces(studyAclKey: AclKey): List<Ace> {
        val now = OffsetDateTime.now()
        return authorizationManager.getAllSecurableObjectPermissions(studyAclKey).aces
            .filter { it.expirationDate.isAfter(now) }
    }

    /**
     * Looks up the directory entry for every user principal in [principals].
     *
     * A principal can outlive its directory entry, and the directory itself is a remote dependency, so anything that
     * can't be resolved is simply left unresolved -- the caller still gets the principal, just without an email
     * address. Failing the whole request would take out the access management screen over a cosmetic lookup.
     */
    private fun resolveDirectoryEntries(principals: Collection<Principal>): Map<Principal, ChronicleUser> {
        val userIds = principals.filter { it.type == PrincipalType.USER }.map { it.id }.toSet()
        if (userIds.isEmpty()) {
            return mapOf()
        }
        return try {
            userDirectoryService.getUsers(userIds).values.associate { user ->
                Principal(PrincipalType.USER, user.id) to user.toChronicleUser()
            }
        } catch (ex: Exception) {
            logger.warn("Unable to resolve {} principal(s) against the user directory.", userIds.size, ex)
            mapOf()
        }
    }

    /**
     * Rejects an update that would leave a study with no owner. Every level of revocation strips OWNER, so an
     * unguarded revoke can orphan a study -- nobody left who can read the acl, let alone grant access back.
     */
    private fun ensureStudyRetainsAnOwner(studyId: UUID, permissionsUpdate: StudyPermissionsUpdate) {
        val revoked = permissionsUpdate.revokeOwnerStudy +
                permissionsUpdate.revokeManageStudy +
                permissionsUpdate.revokeViewStudy

        if (revoked.isEmpty()) {
            return
        }

        val remainingOwners = liveAces(AclKey(studyId))
            .filter { it.permissions.containsAll(STUDY_OWNER_ACCESS) }
            .map { it.principal.id }
            .filterNot { revoked.contains(it) }
            .toSet() + permissionsUpdate.grantOwnerStudy

        // IllegalArgumentException rather than BadRequestException: only the former is mapped to a 400 by
        // ChronicleServerExceptionHandler -- BadRequestException falls through to the catch-all and surfaces as a 500.
        require(remainingOwners.isNotEmpty()) {
            "Study $studyId must have at least one owner. Grant owner access to someone else before revoking the " +
                    "last owner."
        }
    }

    private fun recordStudyPermissionEvents(studyId: UUID, permissionsUpdate: StudyPermissionsUpdate) {
        val events = mutableListOf<AuditableEvent>()

        val granted = mapOf(
            "owner" to permissionsUpdate.grantOwnerStudy,
            "manage" to permissionsUpdate.grantManageStudy,
            "view" to permissionsUpdate.grantViewStudy,
        ).filterValues { it.isNotEmpty() }

        val revoked = mapOf(
            "owner" to permissionsUpdate.revokeOwnerStudy,
            "manage" to permissionsUpdate.revokeManageStudy,
            "view" to permissionsUpdate.revokeViewStudy,
        ).filterValues { it.isNotEmpty() }

        if (granted.isNotEmpty()) {
            events.add(
                AuditableEvent(
                    AclKey(studyId),
                    eventType = AuditEventType.ADD_PERMISSION,
                    description = "Study access granted through StudyApi.updateStudyPermissions",
                    study = studyId,
                    data = mapOf("granted" to granted)
                )
            )
        }

        if (revoked.isNotEmpty()) {
            events.add(
                AuditableEvent(
                    AclKey(studyId),
                    eventType = AuditEventType.REMOVE_PERMISSION,
                    description = "Study access revoked through StudyApi.updateStudyPermissions",
                    study = studyId,
                    data = mapOf("revoked" to revoked)
                )
            )
        }

        if (events.isNotEmpty()) {
            recordEvents(events)
        }
    }

    private fun userPrincipal(userId: String) = Principal(PrincipalType.USER, userId)

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
            AuditedTransactionBuilder<Unit>(conn, auditingManager)
                .transaction { connection -> studyService.updateStudy(connection, studyId, study) }
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
            contact = "support@getmethodic.com",
            definition = DeleteStudyUsageData(studyId)
        )
        val deleteStudyTUDSubmissionJob = ChronicleJob(
            id = idGenerationService.getNextId(),
            contact = "support@getmethodic.com",
            definition = DeleteStudyTUDSubmissionData(studyId)
        )
        val deleteStudyAppUsageSurveyJob = ChronicleJob(
            id = idGenerationService.getNextId(),
            contact = "support@getmethodic.com",
            definition = DeleteStudyAppUsageSurveyData(studyId)
        )
        val jobList = listOf(
            deleteStudyDataJob,
            deleteStudyTUDSubmissionJob,
            deleteStudyAppUsageSurveyJob
        )
        return storageResolver.getPlatformStorage().connection.use { conn ->
            AuditedTransactionBuilder<Iterable<UUID>>(conn, auditingManager)
                .transaction { connection ->
                    val newJobIds = chronicleJobService.createJobs(connection, jobList)
                    logger.info("Created jobs with ids = {}", newJobIds)
                    val studyIdList = listOf(studyId)
                    studyService.deleteStudies(connection, studyIdList)
                    studyService.removeStudiesFromOrganizations(connection, studyIdList)
                    studyService.removeAllParticipantsFromStudies(connection, studyIdList)
                    studies.evict(studyId)
                    return@transaction newJobIds
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
            AuditedTransactionBuilder<Iterable<UUID>>(conn, auditingManager)
                .transaction { connection ->
                    val newJobIds = chronicleJobService.createJobs(connection, jobList)
                    logger.info("Created jobs with ids = {}", newJobIds)
                    studyService.removeParticipantsFromStudy(connection, studyId, participantIds)
                    return@transaction newJobIds
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
        val status = enrollmentManager.getParticipationStatus(studyId, participantId)
        if (ParticipationStatus.NOT_ENROLLED == status) {
            logger.warn(
                "participant is not enrolled, ignoring sensor data upload" + ChronicleServerUtil.STUDY_PARTICIPANT_DATASOURCE,
                studyId,
                participantId,
                sourceDeviceId
            )
            //Don't accumulate an infinite amount of data if a participant accidentally leaves their device running.
            return data.size
        }

        val deviceEnrolled = enrollmentManager.isKnownDatasource(studyId, participantId, sourceDeviceId)

        if (!deviceEnrolled) {
            logger.error(
                "data source not found, ignoring sensor data upload" + ChronicleServerUtil.STUDY_PARTICIPANT_DATASOURCE,
                studyId,
                participantId,
                sourceDeviceId
            )
            return data.size
        }
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
            AuditedTransactionBuilder<Unit>(conn, auditingManager)
                .transaction { connection ->
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
        path = [STUDY_ID_PATH + SETTINGS_PATH + SETTING_TYPE_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getStudySetting(
        @PathVariable(STUDY_ID) studyId: UUID,
        @PathVariable(SETTING_TYPE) settingType: StudySettingType,
    ): StudySetting {
        when (settingType) {
            StudySettingType.Sensor -> ensureValidStudy(studyId)
            else -> ensureReadAccess(AclKey(studyId))
        }
        return studyService.getStudySettings(studyId).getValue(settingType)
    }

    @Deprecated("Prefer getStudySetting, this is left in for app compat.")
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
        produces = [MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE]
    )
    fun getParticipantsData(
        @PathVariable(STUDY_ID) studyId: UUID,
        @RequestParam(value = DATA_TYPE) dataType: ParticipantDataType,
        @RequestParam(value = PARTICIPANT_ID) participantIds: Set<String>,
        @RequestParam(value = START_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) startDateTime: OffsetDateTime?,
        @RequestParam(value = END_DATE) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) endDateTime: OffsetDateTime?,
        @RequestParam(value = RESPONSE_TYPE, defaultValue = "csv") fileType: FileType,
        @RequestParam(value = FILE_NAME) @Size(max = 64) fileName: String?,
        response: HttpServletResponse,
    ): Iterable<Map<String, Any>> {
        check(fileType == FileType.csv || fileType == FileType.json) { "Requested file type must be json or CSV." }
        val data = getParticipantsData(
            studyId,
            dataType,
            participantIds,
            MoreObjects.firstNonNull(startDateTime, OffsetDateTime.MIN),
            MoreObjects.firstNonNull(endDateTime, OffsetDateTime.MAX)
        )

        ChronicleServerUtil.setDownloadContentType(response, fileType)
        ChronicleServerUtil.setContentDisposition(
            response,
            MoreObjects.firstNonNull(
                fileName,
                "${dataType}_${
                    LocalDate.now()
                        .format(DateTimeFormatter.BASIC_ISO_DATE)
                }"
            ),
            fileType
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
