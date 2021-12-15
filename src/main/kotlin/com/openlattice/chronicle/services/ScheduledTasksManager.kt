package com.openlattice.chronicle.services

import com.dataloom.streams.StreamUtil
import com.google.common.collect.*
import com.openlattice.chronicle.constants.AppComponent
import com.openlattice.chronicle.constants.EdmConstants
import com.openlattice.chronicle.constants.RecordType
import com.openlattice.chronicle.services.entitysets.EntitySetIdsManager
import com.openlattice.chronicle.util.ChronicleServerUtil
import com.openlattice.client.ApiClient
import com.openlattice.data.DataApi
import com.openlattice.data.requests.EntitySetSelection
import com.openlattice.data.requests.FileType
import com.openlattice.data.requests.NeighborEntityDetails
import com.openlattice.search.requests.EntityNeighborsFilter
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.util.*
import java.util.function.Consumer
import java.util.stream.Collectors


private const val USER_APPS_REFRESH_INTERVAL = 60 * 1000L // 1 minute
private const  val STUDY_INFO_REFRESH_INTERVAL = (60 * 1000L) // 1 minute
private const  val SYNC_USER_REFRESH_INTERVAL = (60 * 1000L) // 1 minute
private const  val SYSTEM_APPS_REFRESH_INTERVAL = (60 * 60 * 1000L) // 1 hour
private const val DEVICES_INFO_REFRESH_INTERVAL = (60 * 1000L) // 1 minute

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class ScheduledTasksManager {
    private val logger = LoggerFactory.getLogger(ScheduledTasksManager::class.java)


    // orgId -> studyId -> participantId -> EKID
    private val studyParticipantsByOrg: MutableMap<UUID, Map<UUID?, MutableMap<String, UUID>>> = Maps.newHashMap()

    // orgId -> studyId -> studyEKID
    private val studyEntityKeyIdsByOrg: MutableMap<UUID, MutableMap<UUID, UUID>> = Maps.newHashMap()

    // orgId -> deviceId -> deviceEKID
    private val deviceIdsByOrg: MutableMap<UUID, Map<String, UUID>> = Maps.newHashMap()

    // app fullName -> { org1, org2, org3 }
    private val userAppsFullNamesByOrg: MutableMap<String, MutableSet<UUID>> = Maps.newLinkedHashMap()

    // deviceId -> deviceEKID
    private val deviceIdsByEKID: MutableMap<String, UUID> = Maps.newHashMap()

    // studyId -> participantId -> participant EKID
    private val studyParticipants: MutableMap<UUID, MutableMap<String, UUID>> = Maps.newHashMap()

    // studyId -> studyEKID
    private val studyEKIDById: MutableMap<UUID, UUID> = Maps.newHashMap()

    // app fullnames in chronicle_user_apps entity set
    private val userAppsFullNameValues: MutableSet<String> = Sets.newHashSet()

    // app fullnames in chronicle_application_dictionary entity set
    private val systemAppPackageNames: MutableSet<String> = Sets.newHashSet()


    @Scheduled(fixedRate = SYSTEM_APPS_REFRESH_INTERVAL)
    fun refreshSystemApps() {
        logger.info("Refreshing system apps cache")
        TODO("Not yet implemented")
    }

    @Scheduled(fixedRate = USER_APPS_REFRESH_INTERVAL)
    @Deprecated("")
    fun legacyRefreshUserAppsFullNames() {
        logger.info("refreshing user apps cache")
        TODO("Not yet implemented")
    }

    @Scheduled(fixedRate = USER_APPS_REFRESH_INTERVAL)
    fun refreshAllOrgsUserAppFullNames() {
        logger.info("refreshing all orgs user apps fullnames")
        TODO("Not yet implemented")
    }

    @Scheduled(fixedRate = STUDY_INFO_REFRESH_INTERVAL)
    fun refreshAllOrgsStudyInformation() {
        logger.info("refreshing study information for all organizations")
        TODO("Not yet implemented")
    }

    @Scheduled(fixedRate = STUDY_INFO_REFRESH_INTERVAL)
    @Deprecated("")
    fun legacyRefreshStudyInformation() {
        logger.info("refreshing cache for legacy studies")
        TODO("Not yet implemented")
    }

    @Scheduled(fixedRate = DEVICES_INFO_REFRESH_INTERVAL)
    fun refreshAllOrgsDevicesCache() {
        logger.info("refreshing devices info for all orgs")
        TODO("Not yet implemented")
    }

    @Scheduled(fixedRate = DEVICES_INFO_REFRESH_INTERVAL)
    @Deprecated("")
    fun legacyRefreshDevicesCache() {
        logger.info("refreshing devices info for legacy devices")
        TODO("Not yet implemented")
    }

    fun getStudyParticipantsByOrg(): Map<UUID, Map<UUID?, MutableMap<String, UUID>>> {
        return studyParticipantsByOrg
    }

    fun getStudyEntityKeyIdsByOrg(): Map<UUID, MutableMap<UUID, UUID>> {
        return studyEntityKeyIdsByOrg
    }

    fun getDeviceIdsByOrg(): Map<UUID, Map<String, UUID>> {
        return deviceIdsByOrg
    }

    fun getUserAppsFullNamesByOrg(): Map<String, MutableSet<UUID>> {
        return userAppsFullNamesByOrg
    }

    fun getDeviceIdsByEKID(): Map<String, UUID> {
        return deviceIdsByEKID
    }

    fun getStudyParticipants(): Map<UUID, MutableMap<String, UUID>> {
        return studyParticipants
    }

    fun getStudyEKIDById(): Map<UUID, UUID> {
        return studyEKIDById
    }

    fun getUserAppsFullNameValues(): Set<String> {
        return userAppsFullNameValues
    }

    fun getSystemAppPackageNames(): Set<String> {
        return systemAppPackageNames
    }

    init {
        refreshAllOrgsUserAppFullNames()
        legacyRefreshUserAppsFullNames()
        refreshAllOrgsStudyInformation()
        refreshSystemApps()
        legacyRefreshDevicesCache()
        refreshAllOrgsDevicesCache()
        legacyRefreshStudyInformation()
    }
}
