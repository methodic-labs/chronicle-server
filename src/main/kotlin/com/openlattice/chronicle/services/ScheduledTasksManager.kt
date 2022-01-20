package com.openlattice.chronicle.services

import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.google.common.collect.Maps
import com.google.common.collect.Sets
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.SYSTEM_APPS
import com.openlattice.chronicle.storage.StorageResolver
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.util.*


private const val USER_APPS_REFRESH_INTERVAL = 60 * 1000L // 1 minute
private const val STUDY_INFO_REFRESH_INTERVAL = (60 * 1000L) // 1 minute
private const val SYNC_USER_REFRESH_INTERVAL = (60 * 1000L) // 1 minute
private const val SYSTEM_APPS_REFRESH_INTERVAL = (60 * 60 * 1000L) // 1 hour
private const val DEVICES_INFO_REFRESH_INTERVAL = (60 * 1000L) // 1 minute

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class ScheduledTasksManager(
        private val storageResolver: StorageResolver
) {

    companion object {
        private val logger = LoggerFactory.getLogger(ScheduledTasksManager::class.java)

        private val GET_SYSTEM_APPS_SQL = """
            SELECT * FROM ${SYSTEM_APPS.name}
        """.trimIndent()
    }



    // orgId -> studyId -> participantId -> EKID
    val studyParticipantsByOrg: MutableMap<UUID, Map<UUID?, MutableMap<String, UUID>>> = Maps.newHashMap()

    // orgId -> studyId -> studyEKID
    val studyEntityKeyIdsByOrg: MutableMap<UUID, MutableMap<UUID, UUID>> = Maps.newHashMap()

    // orgId -> deviceId -> deviceEKID
    val deviceIdsByOrg: MutableMap<UUID, Map<String, UUID>> = Maps.newHashMap()

    // app fullName -> { org1, org2, org3 }
    val userAppsFullNamesByOrg: MutableMap<String, MutableSet<UUID>> = Maps.newLinkedHashMap()

    // deviceId -> deviceEKID
    val deviceIdsByEKID: MutableMap<String, UUID> = Maps.newHashMap()

    // studyId -> participantId -> participant EKID
    val studyParticipants: MutableMap<UUID, MutableMap<String, UUID>> = Maps.newHashMap()

    // studyId -> studyEKID
    val studyEKIDById: MutableMap<UUID, UUID> = Maps.newHashMap()

    // app fullnames in chronicle_user_apps entity set
    val userAppsFullNameValues: MutableSet<String> = Sets.newHashSet()

    // app fullnames in chronicle_application_dictionary entity set
    val systemAppPackageNames: MutableSet<String> = Sets.newHashSet()


    @Scheduled(fixedRate = SYSTEM_APPS_REFRESH_INTERVAL)
    fun refreshSystemApps() {
        logger.info("Refreshing system apps cache")

        val (_, hds ) = storageResolver.getDefaultStorage()

        val systemAppNames = BasePostgresIterable(
                PreparedStatementHolderSupplier(hds, GET_SYSTEM_APPS_SQL) { ps ->
                    ps.executeQuery()
                }
        ) {
            ResultSetAdapters.systemApp(it)
        }.toSet()

        logger.info("loaded ${systemAppNames.size} system apps")

        systemAppPackageNames.addAll(systemAppNames)
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
}
