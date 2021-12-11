package com.openlattice.chronicle.services.upload

import com.dataloom.streams.StreamUtil
import com.geekbeast.configuration.postgres.PostgresFlavor
import com.geekbeast.util.StopWatch
import com.google.common.collect.*
import com.openlattice.ApiUtil
import com.openlattice.chronicle.constants.AppComponent
import com.openlattice.chronicle.constants.AppUsageFrequencyType
import com.openlattice.chronicle.constants.EdmConstants
import com.openlattice.chronicle.constants.EdmConstants.*
import com.openlattice.chronicle.constants.OutputConstants
import com.openlattice.chronicle.data.EntitiesAndEdges
import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.services.ScheduledTasksManager
import com.openlattice.chronicle.services.edm.EdmCacheManager
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.services.entitysets.EntitySetIdsManager
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.FQNS_TO_COLUMNS
import com.openlattice.chronicle.storage.RedshiftTables.Companion.CHRONICLE_USAGE_EVENTS
import com.openlattice.chronicle.storage.RedshiftTables.Companion.getAppendTembTableSql
import com.openlattice.chronicle.storage.RedshiftTables.Companion.getDeleteTempTableEntriesSql
import com.openlattice.chronicle.storage.RedshiftTables.Companion.getInsertIntoMergeUsageEventsTableSql
import com.openlattice.chronicle.storage.RedshiftTables.Companion.getInsertUsageEventColumnIndex
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.util.ChronicleServerUtil
import com.openlattice.data.*
import com.openlattice.postgres.PostgresColumnDefinition
import com.openlattice.postgres.PostgresDatatype
import com.zaxxer.hikari.HikariDataSource
import org.apache.commons.lang3.tuple.Triple
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.slf4j.event.Level
import java.security.InvalidParameterException
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.function.Consumer
import java.util.stream.Collectors

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

class AppDataUploadService(
        private val storageResolver: StorageResolver,
        private val entitySetIdsManager: EntitySetIdsManager,
        private val scheduledTasksManager: ScheduledTasksManager,
        private val enrollmentManager: EnrollmentManager,
        private val edmCacheManager: EdmCacheManager
) : AppDataUploadManager {
    private val logger = LoggerFactory.getLogger(AppDataUploadService::class.java)

    private fun getTruncatedDateTimeHelper(dateTime: String?, chronoUnit: ChronoUnit): String? {
        return if (dateTime == null) {
            null
        } else OffsetDateTime.parse(dateTime)
                .truncatedTo(chronoUnit)
                .toString()
    }

    private fun getTruncatedDateTime(datetime: String, organizationId: UUID): String? {
        val settings = entitySetIdsManager
                .getOrgAppSettings(AppComponent.CHRONICLE_DATA_COLLECTION, organizationId)
        val appUsageFreq = settings.getOrDefault(APP_USAGE_FREQUENCY, AppUsageFrequencyType.DAILY).toString()
        val chronoUnit = if (appUsageFreq == AppUsageFrequencyType.HOURLY.toString()) ChronoUnit.HOURS else ChronoUnit.DAYS
        return getTruncatedDateTimeHelper(datetime, chronoUnit)
    }

    // unique for user + app + date
    private fun getUsedByEntityKey(usedByESID: UUID, entityData: Map<UUID, Set<Any?>>): EntityKey {
        return EntityKey(
                usedByESID,
                ApiUtil.generateDefaultEntityId(
                        ImmutableList.of(
                                edmCacheManager.getPropertyTypeId(EdmConstants.FULL_NAME_FQN),
                                edmCacheManager.getPropertyTypeId(EdmConstants.DATE_TIME_FQN),
                                edmCacheManager.getPropertyTypeId(EdmConstants.PERSON_ID_FQN)
                        ),
                        entityData
                )
        )
    }

    // unique for app + device + date
    private fun getRecordedByEntityKey(recordedByESID: UUID, entityData: Map<UUID, Set<Any?>>): EntityKey {
        return EntityKey(
                recordedByESID,
                ApiUtil.generateDefaultEntityId(
                        ImmutableList.of(
                                edmCacheManager.getPropertyTypeId(EdmConstants.DATE_LOGGED_FQN),
                                edmCacheManager.getPropertyTypeId(EdmConstants.STRING_ID_FQN),
                                edmCacheManager.getPropertyTypeId(EdmConstants.FULL_NAME_FQN)
                        ),
                        entityData
                )
        )
    }

    private fun getUserAppsEntityKey(userAppsESID: UUID, entityData: Map<UUID, Set<Any?>>): EntityKey {
        return EntityKey(
                userAppsESID,
                ApiUtil.generateDefaultEntityId(
                        ImmutableList
                                .of(edmCacheManager.getPropertyTypeId(EdmConstants.FULL_NAME_FQN)),
                        entityData
                )
        )
    }

    private fun getMetadataEntityKey(metadataESID: UUID, entityData: Map<UUID, Set<Any>>): EntityKey {
        return EntityKey(
                metadataESID,
                ApiUtil.generateDefaultEntityId(
                        ImmutableList.of(
                                edmCacheManager.getPropertyTypeId(EdmConstants.OL_ID_FQN)
                        ),
                        entityData
                )
        )
    }

    private fun getHasEntityKey(hasESID: UUID, entityData: Map<UUID, Set<Any>>): EntityKey {
        return EntityKey(
                hasESID,
                ApiUtil.generateDefaultEntityId(
                        ImmutableList.of(
                                edmCacheManager.getPropertyTypeId(EdmConstants.OL_ID_FQN)
                        ),
                        entityData
                )
        )
    }

    // HELPER METHODS: upload
    private fun getUsedByEntity(
            appPackageName: String?, dateLogged: String, participantId: String
    ): MutableMap<UUID, Set<Any?>> {
        val entity: MutableMap<UUID, Set<Any?>> = Maps.newHashMap()
        entity[edmCacheManager.getPropertyTypeId(EdmConstants.DATE_TIME_FQN)] = ImmutableSet.of<Any?>(dateLogged)
        entity[edmCacheManager.getPropertyTypeId(EdmConstants.FULL_NAME_FQN)] = ImmutableSet.of<Any?>(appPackageName)
        entity[edmCacheManager.getPropertyTypeId(EdmConstants.PERSON_ID_FQN)] = ImmutableSet.of<Any?>(participantId)
        return entity
    }

    private fun getRecordedByEntity(
            deviceId: String, appPackageName: String?, dateLogged: String
    ): MutableMap<UUID, Set<Any?>> {
        val entity: MutableMap<UUID, Set<Any?>> = Maps.newHashMap()
        entity[edmCacheManager.getPropertyTypeId(EdmConstants.FULL_NAME_FQN)] = ImmutableSet.of<Any?>(appPackageName)
        entity[edmCacheManager.getPropertyTypeId(EdmConstants.DATE_LOGGED_FQN)] = ImmutableSet.of<Any?>(dateLogged)
        entity[edmCacheManager.getPropertyTypeId(EdmConstants.STRING_ID_FQN)] = ImmutableSet.of<Any?>(deviceId)
        return entity
    }

    private fun getHasEntity(participantEKID: UUID): Map<UUID, Set<Any>> {
        val entity: MutableMap<UUID, Set<Any>> = Maps.newHashMap()
        entity[edmCacheManager.getPropertyTypeId(EdmConstants.OL_ID_FQN)] = java.util.Set.of<Any>(participantEKID)
        return entity
    }

    private fun getMetadataEntity(participantEKID: UUID): MutableMap<UUID, Set<Any>> {
        val entity: MutableMap<UUID, Set<Any>> = Maps.newHashMap()
        entity[edmCacheManager.getPropertyTypeId(EdmConstants.OL_ID_FQN)] = java.util.Set.of<Any>(participantEKID)
        return entity
    }

    private fun getUserAppsEntity(appPackageName: String?, appName: String?): Map<UUID, Set<Any?>> {
        val entity: MutableMap<UUID, Set<Any?>> = Maps.newHashMap()
        entity[edmCacheManager.getPropertyTypeId(EdmConstants.FULL_NAME_FQN)] = ImmutableSet.of<Any?>(appPackageName)
        entity[edmCacheManager.getPropertyTypeId(EdmConstants.TITLE_FQN)] = ImmutableSet.of<Any?>(appName)
        return entity
    }

    private fun getDeviceEntityKey(devicesESID: UUID, deviceId: String): EntityKey {
        return EntityKey(
                devicesESID,
                deviceId
        )
    }

    private fun getParticipantEntityKey(participantESID: UUID, participantId: String): EntityKey {
        return EntityKey(
                participantESID,
                participantId
        )
    }

    private fun getDateTimeValuesFromDeviceData(data: List<SetMultimap<UUID, Any>>): Set<OffsetDateTime> {
        val dateTimes: MutableSet<OffsetDateTime> = Sets.newHashSet()
        data.forEach(
                Consumer { entity: SetMultimap<UUID, Any> ->
                    // most date properties in the entity are of length 1
                    for (date in entity[edmCacheManager.getPropertyTypeId(
                            EdmConstants.DATE_LOGGED_FQN
                    )]) {
                        val parsedDateTime = OffsetDateTime
                                .parse(date.toString())

                        // filter out problematic entities with dates in the sixties
                        if (parsedDateTime.isAfter(OutputConstants.MINIMUM_DATE)) {
                            dateTimes.add(parsedDateTime)
                        }
                    }
                }
        )
        return dateTimes
    }

    private fun getMetadataEntitiesAndEdges(
            dataApi: DataApi,
            integrationApi: DataIntegrationApi,
            data: List<SetMultimap<UUID, Any>>,
            organizationId: UUID,
            studyId: UUID,
            participantESID: UUID,
            participantEKID: UUID,
            participantId: String
    ): EntitiesAndEdges? {
        // entity set ids
        val coreAppConfig = entitySetIdsManager.getChronicleAppConfig(organizationId)
        val metadataESID = coreAppConfig.metadataEntitySetId
        val hasESID = coreAppConfig.hasEntitySetId
        val entitiesByEntityKey: MutableMap<EntityKey, Map<UUID, Set<Any>>> = Maps.newHashMap()
        val edgesByEntityKey: MutableSet<Triple<EntityKey, EntityKey, EntityKey>> = Sets.newHashSet()

        // get all dates in new data batch
        val pushedDateTimes = getDateTimeValuesFromDeviceData(data)
        if (pushedDateTimes.size == 0) {
            return null
        }
        val firstDateTime = pushedDateTimes
                .stream()
                .min { obj: OffsetDateTime, other: OffsetDateTime? ->
                    obj.compareTo(
                            other
                    )
                } // .orElse( null ) :commenting this out since pushedDateTimes can never have nulls
                .get()
                .toString()
        val lastDateTime = pushedDateTimes
                .stream()
                .max { obj: OffsetDateTime, other: OffsetDateTime? ->
                    obj.compareTo(
                            other
                    )
                } // .orElse( null ) :commenting this out since pushedDateTimes can never have nulls
                .get()
                .toString()
        val uniqueDates = pushedDateTimes
                .stream()
                .map { dt: OffsetDateTime ->
                    dt
                            .truncatedTo(ChronoUnit.DAYS)
                            .format(DateTimeFormatter.ISO_DATE_TIME)
                }
                .collect(Collectors.toSet())
        val metadataEntityData = getMetadataEntity(participantEKID)
        val metadataEK = getMetadataEntityKey(metadataESID, metadataEntityData)

        // verify if there is already an entry of metadata for participant
        // error means there is no metadata yet.
        val metadataEntityKeyId = integrationApi.getEntityKeyIds(ImmutableSet.of(metadataEK)).iterator().next()
        try {
            val entity = dataApi.getEntity(metadataESID, metadataEntityKeyId)
            metadataEntityData[edmCacheManager.getPropertyTypeId(
                    START_DATE_TIME_FQN
            )] = entity.getOrDefault(
                    START_DATE_TIME_FQN, java.util.Set.of<Any>(firstDateTime)
            )
        } catch (exception: Exception) {
            metadataEntityData[edmCacheManager.getPropertyTypeId(
                    START_DATE_TIME_FQN
            )] = ImmutableSet.of<Any>(firstDateTime)
            logger.error(
                    "failure while getting metadata entity = {}" + ChronicleServerUtil.ORG_STUDY_PARTICIPANT,
                    metadataEntityKeyId,
                    organizationId,
                    studyId,
                    participantId,
                    exception
            )
        }
        metadataEntityData[edmCacheManager.getPropertyTypeId(EdmConstants.RECORDED_DATE_TIME_FQN)] = uniqueDates
        entitiesByEntityKey[metadataEK] = metadataEntityData

        // Update endDateTime separately with PartialReplace to prevent the data array from growing linearly with the # of uploads
        val lastDateEntity: Map<UUID, Set<Any>> = ImmutableMap
                .of<UUID, Set<Any>>(
                        edmCacheManager.getPropertyTypeId(EdmConstants.END_DATE_TIME_FQN),
                        ImmutableSet.of<Any>(lastDateTime)
                )
        dataApi.updateEntitiesInEntitySet(
                metadataESID,
                ImmutableMap.of(metadataEntityKeyId, lastDateEntity),
                UpdateType.PartialReplace,
                PropertyUpdateType.Versioned
        )
        val hasEntityData = getHasEntity(participantEKID)
        val hasEK = getHasEntityKey(hasESID, hasEntityData)
        entitiesByEntityKey[hasEK] = hasEntityData
        val participantEK = getParticipantEntityKey(participantESID, participantId)

        // association: participant  => has => metadata
        edgesByEntityKey.add(Triple.of(participantEK, hasEK, metadataEK))
        return EntitiesAndEdges(entitiesByEntityKey, edgesByEntityKey)
    }

    private fun getFirstValueOrNull(entity: SetMultimap<UUID, Any>, fqn: FullQualifiedName): String? {
        val fqnId = edmCacheManager.getPropertyTypeId(fqn)
        val value = Iterables.getFirst(entity[fqnId], null)
        return value?.toString()
    }

    private fun getEntityKeyIdMap(
            integrationApi: DataIntegrationApi,
            edgesByEntityKey: Set<Triple<EntityKey, EntityKey, EntityKey>>,
            entityKeys: Set<EntityKey>,
            participantESID: UUID,
            devicesESID: UUID,
            deviceEKID: UUID,
            participantEKID: UUID,
            participantId: String,
            deviceId: String
    ): Map<EntityKey, UUID> {
        val entityKeyIdMap: MutableMap<EntityKey, UUID> = Maps.newHashMap()
        val orderedEntityKeys: MutableSet<EntityKey> = Sets.newLinkedHashSet(entityKeys)
        edgesByEntityKey.forEach(
                Consumer { triple: Triple<EntityKey, EntityKey, EntityKey> ->
                    orderedEntityKeys.add(triple.middle)
                    orderedEntityKeys.add(triple.left)
                    orderedEntityKeys.add(triple.right)
                })
        val entityKeyIds = integrationApi.getEntityKeyIds(orderedEntityKeys)
        val entityKeyList: List<EntityKey> = ArrayList(orderedEntityKeys)
        for (i in orderedEntityKeys.indices) {
            entityKeyIdMap[entityKeyList[i]] = entityKeyIds[i]
        }

        // others
        val participantEK = getParticipantEntityKey(participantESID, participantId)
        entityKeyIdMap[participantEK] = participantEKID
        val deviceEK = getDeviceEntityKey(devicesESID, deviceId)
        entityKeyIdMap[deviceEK] = deviceEKID
        return entityKeyIdMap
    }

    // group entities by entity set id
    private fun groupEntitiesByEntitySetId(
            entitiesByEntityKey: Map<EntityKey, Map<UUID, Set<Any?>>>,
            entityKeyIdMap: Map<EntityKey, UUID>
    ): Map<UUID, MutableMap<UUID?, Map<UUID, Set<Any?>>>> {
        val entityKeysByEntitySet: MutableMap<UUID, MutableMap<UUID?, Map<UUID, Set<Any?>>>> = Maps.newHashMap()
        entitiesByEntityKey.forEach { (entityKey: EntityKey, entity: Map<UUID, Set<Any?>>) ->
            val entitySetId = entityKey.entitySetId
            val entityKeyId = entityKeyIdMap[entityKey]
            val mappedEntity = entityKeysByEntitySet
                    .getOrDefault(
                            entitySetId, Maps.newHashMap()
                    )
            mappedEntity[entityKeyId] = entity
            entityKeysByEntitySet[entitySetId] = mappedEntity
        }
        return entityKeysByEntitySet
    }

    private fun getDataEdgeKeysFromEntityKeys(
            edgesByEntityKey: Set<Triple<EntityKey, EntityKey, EntityKey>>,
            entityKeyIdMap: Map<EntityKey, UUID>
    ): Set<DataEdgeKey> {
        return StreamUtil.stream(edgesByEntityKey)
                .map { triple: Triple<EntityKey, EntityKey, EntityKey> ->
                    val srcEKID = entityKeyIdMap[triple.left]
                    val edgeEKID = entityKeyIdMap[triple.middle]
                    val dstEKID = entityKeyIdMap[triple.right]
                    val srcESID = triple.left.entitySetId
                    val edgeESID = triple.middle.entitySetId
                    val dstESID = triple.right.entitySetId
                    val src = EntityDataKey(srcESID, srcEKID)
                    val edge = EntityDataKey(edgeESID, edgeEKID)
                    val dst = EntityDataKey(dstESID, dstEKID)
                    DataEdgeKey(src, dst, edge)
                }
                .collect(Collectors.toSet())
    }

    private fun getAppDataAssociations(
            deviceEntityKeyId: UUID,
            participantEntityKeyId: UUID,
            appDataESID: UUID,
            devicesESID: UUID,
            recordedByESID: UUID,
            participantEntitySetId: UUID,
            timeStamp: OffsetDateTime,
            index: Int
    ): ListMultimap<UUID, DataAssociation> {
        val associations: ListMultimap<UUID, DataAssociation> = ArrayListMultimap.create()
        val recordedByEntity: Map<UUID, Set<Any>> = ImmutableMap
                .of<UUID, Set<Any>>(
                        edmCacheManager.getPropertyTypeId(EdmConstants.DATE_LOGGED_FQN), Sets.newHashSet<Any>(timeStamp)
                )
        associations.put(
                recordedByESID, DataAssociation(
                appDataESID,
                Optional.of(index),
                Optional.empty(),
                devicesESID,
                Optional.empty(),
                Optional.of(deviceEntityKeyId),
                recordedByEntity
        )
        )
        associations.put(
                recordedByESID, DataAssociation(
                appDataESID,
                Optional.of(index),
                Optional.empty(),
                participantEntitySetId,
                Optional.empty(),
                Optional.of(participantEntityKeyId),
                recordedByEntity
        )
        )
        return associations
    }

    private fun hasUserAppPackageName(organizationId: UUID?, packageName: String?): Boolean {
        return if (organizationId != null) {
            scheduledTasksManager.userAppsFullNamesByOrg.getOrDefault(packageName, ImmutableSet.of())
                    .contains(organizationId)
        } else scheduledTasksManager.userAppsFullNameValues.contains(packageName)
    }

    // create entities and edges
    private fun createEntitiesAndAssociations(
            dataApi: DataApi,
            dataIntegrationApi: DataIntegrationApi,
            data: List<SetMultimap<UUID, Any>>,
            organizationId: UUID,
            studyId: UUID,
            deviceEKID: UUID,
            participantId: String,
            deviceId: String,
            participantEKID: UUID
    ) {
        // entity set ids
        val coreAppConfig = entitySetIdsManager
                .getChronicleAppConfig(organizationId, ChronicleServerUtil.getParticipantEntitySetName(studyId))
        val dataCollectionAppConfig = entitySetIdsManager
                .getChronicleDataCollectionAppConfig(organizationId)
        val appDataESID = dataCollectionAppConfig.appDataEntitySetId
        val recordedByESID = dataCollectionAppConfig.recordedByEntitySetId
        val devicesESID = dataCollectionAppConfig.deviceEntitySetId
        val usedByESID = dataCollectionAppConfig.usedByEntitySetId
        val userAppsESID = dataCollectionAppConfig.userAppsEntitySetId
        val participantESID = coreAppConfig.participantEntitySetId
        val appDataEntities: ListMultimap<UUID, Map<UUID, Set<Any>>> = ArrayListMultimap.create()
        val appDataAssociations: ListMultimap<UUID, DataAssociation> = ArrayListMultimap.create()
        val entitiesByEntityKey: MutableMap<EntityKey, Map<UUID, Set<Any?>>> = Maps.newHashMap()
        val edgesByEntityKey: MutableSet<Triple<EntityKey, EntityKey, EntityKey>> = Sets.newHashSet()
        val timeStamp = OffsetDateTime.now()
        for (i in data.indices) {
            val appEntity = data[i]
            appDataEntities.put(appDataESID, Multimaps.asMap(appEntity))
            appDataAssociations
                    .putAll(
                            getAppDataAssociations(
                                    deviceEKID,
                                    participantEKID,
                                    appDataESID,
                                    devicesESID,
                                    recordedByESID,
                                    participantESID,
                                    timeStamp,
                                    i
                            )
                    )
            var appPackageName: String?
            var appName: String?
            appName = getFirstValueOrNull(appEntity, EdmConstants.FULL_NAME_FQN)
            appPackageName = appName
            val eventDate = getFirstValueOrNull(appEntity, EdmConstants.DATE_LOGGED_FQN)
                    ?: continue
            val dateLogged = getTruncatedDateTime(eventDate, organizationId)
            if (scheduledTasksManager.systemAppPackageNames.contains(appPackageName) || dateLogged == null) {
                continue  // 'system' app
            }
            if (appEntity.containsKey(edmCacheManager.getPropertyTypeId(EdmConstants.TITLE_FQN))) {
                appName = getFirstValueOrNull(appEntity, EdmConstants.TITLE_FQN)
            }

            // association 1: user apps => recorded by => device
            val userAppEntityData = getUserAppsEntity(appPackageName, appName)
            val userAppEK = getUserAppsEntityKey(userAppsESID, userAppEntityData)
            if (!hasUserAppPackageName(organizationId, appPackageName)) {
                entitiesByEntityKey[userAppEK] = userAppEntityData
            }
            val recordedByEntityData = getRecordedByEntity(deviceId, appPackageName, dateLogged)
            val recordedByEK = getRecordedByEntityKey(recordedByESID, recordedByEntityData)
            recordedByEntityData
                    .remove(
                            edmCacheManager
                                    .getPropertyTypeId(EdmConstants.FULL_NAME_FQN)
                    ) // FULL_NAME_FQN is used to generate EKID but shouldn't be stored
            entitiesByEntityKey[recordedByEK] = recordedByEntityData
            val deviceEK = getDeviceEntityKey(devicesESID, deviceId)
            edgesByEntityKey.add(Triple.of(userAppEK, recordedByEK, deviceEK))

            // association 2: user apps => used by => participant
            val usedByEntityData = getUsedByEntity(appPackageName, dateLogged, participantId)
            val usedByEK = getUsedByEntityKey(usedByESID, usedByEntityData)
            usedByEntityData.remove(
                    edmCacheManager
                            .getPropertyTypeId(EdmConstants.FULL_NAME_FQN)
            ) // FULL_NAME_FQN shouldn't be stored
            usedByEntityData.remove(
                    edmCacheManager
                            .getPropertyTypeId(EdmConstants.PERSON_ID_FQN)
            ) // PERSON_ID_FQN shouldn't be stored

            // we generate the entity key id using a truncated date to enforce uniqueness, but we'll store the actual datetime value
            usedByEntityData[edmCacheManager.getPropertyTypeId(
                    EdmConstants.DATE_TIME_FQN
            )] = ImmutableSet.of<Any?>(eventDate)
            entitiesByEntityKey[usedByEK] = usedByEntityData
            val participantEK = getParticipantEntityKey(participantESID, participantId)
            edgesByEntityKey.add(Triple.of(userAppEK, usedByEK, participantEK))
        }

        val metadata = getMetadataEntitiesAndEdges(
                dataApi,
                dataIntegrationApi,
                data,
                organizationId,
                studyId,
                participantESID,
                participantEKID,
                participantId
        )
        if (metadata != null) {
            entitiesByEntityKey.putAll(metadata.entityByEntityKey)
            edgesByEntityKey.addAll(metadata.srcEdgeDstEntityKeys)
        }
        val dataGraph = DataGraph(appDataEntities, appDataAssociations)
        dataApi.createEntityAndAssociationData(dataGraph)
        val entityKeyIdMap = getEntityKeyIdMap(
                dataIntegrationApi,
                edgesByEntityKey,
                entitiesByEntityKey.keys,
                participantESID,
                devicesESID,
                deviceEKID,
                participantEKID,
                participantId,
                deviceId
        )
        val entitiesByESID = groupEntitiesByEntitySetId(
                entitiesByEntityKey,
                entityKeyIdMap
        )
        entitiesByESID.forEach { (entitySetId: UUID?, entities: Map<UUID?, Map<UUID, Set<Any?>>>?) ->
            dataApi.updateEntitiesInEntitySet(
                    entitySetId, entities, UpdateType.Merge, PropertyUpdateType.Versioned
            )
        }
        val dataEdgeKeys = getDataEdgeKeysFromEntityKeys(edgesByEntityKey, entityKeyIdMap)
        dataApi.createEdges(dataEdgeKeys)
    }

    /**
     * This routine implements once and only once append of client data.
     *
     * Assumptions:
     * - Client generates a UUID uniformly at random for each event and stores it in the id field.
     * - Client will retry upload until receives successful acknowledgement from the server.
     *
     * Data is first written into a postgres table which is periodically flushed to redshift for long term storage.
     *
     * The probability of the same UUID being generated twice for the same organization id/participant id/device
     * id/timestamp is unlikely to happen in the lifetime of our universe.
     */
    override fun upload(
            organizationId: UUID,
            studyId: UUID,
            participantId: String,
            dataSourceId: String,
            data: List<SetMultimap<UUID, Any>>
    ): Int {
        StopWatch(
                log = "logging ${data.size} entries for ${ChronicleServerUtil.ORG_STUDY_PARTICIPANT_DATASOURCE}",
                level = Level.INFO,
                logger = logger,
                data.size,
                organizationId,
                studyId,
                participantId,
                dataSourceId
        ).use {
            try {
                val (flavor, hds) = storageResolver.resolve(studyId)
                val participantEntityKeyId = enrollmentManager
                        .getParticipantEntityKeyId(organizationId, studyId, participantId)
                if (participantEntityKeyId == null) {
                    logger.error(
                            "unable to get participant ekid" + ChronicleServerUtil.ORG_STUDY_PARTICIPANT_DATASOURCE,
                            organizationId,
                            studyId,
                            participantId,
                            dataSourceId
                    )
                    return 0
                }
                val status = enrollmentManager
                        .getParticipationStatus(organizationId, studyId, participantId)
                if (ParticipationStatus.NOT_ENROLLED == status) {
                    logger.warn(
                            "participant is not enrolled, ignoring upload" + ChronicleServerUtil.ORG_STUDY_PARTICIPANT_DATASOURCE,
                            organizationId,
                            studyId,
                            participantId,
                            dataSourceId
                    )
                    return 0
                }
                val deviceEntityKeyId = enrollmentManager
                        .getDeviceEntityKeyId(organizationId, studyId, participantId, dataSourceId)
                if (deviceEntityKeyId == null) {
                    logger.error(
                            "data source not found, ignoring upload" + ChronicleServerUtil.ORG_STUDY_PARTICIPANT_DATASOURCE,
                            organizationId,
                            studyId,
                            participantId,
                            dataSourceId
                    )
                    return 0
                }

                logger.info(
                        "attempting to log data" + ChronicleServerUtil.ORG_STUDY_PARTICIPANT_DATASOURCE,
                        organizationId,
                        studyId,
                        participantId,
                        dataSourceId
                )

                val mappedData = filter(organizationId, mapToStorageModel(data))

                StopWatch( log = "Writing ${data.size} entites to DB ")
                val written = when (flavor) {
                    PostgresFlavor.VANILLA -> writeToPostgres(hds, organizationId, studyId, participantId, mappedData)
                    PostgresFlavor.REDSHIFT -> writeToRedshift(hds, organizationId, studyId, participantId, mappedData)
                    else -> throw InvalidParameterException("Only regular postgres and redshift are supported.")
                }

                //TODO: In reality we are likely to write less entities than were provided and are actually returning number processed so that client knows all is good
                if (data.size != written) {
                    //Should probably be an assertion as this should never happen.
                    logger.warn("Wrote $written entities, but expected to write ${data.size} entities")
                }
                return data.size
            } catch (exception: Exception) {
                logger.error(
                        "error logging data" + ChronicleServerUtil.ORG_STUDY_PARTICIPANT_DATASOURCE,
                        organizationId,
                        studyId,
                        participantId,
                        dataSourceId,
                        exception
                )
                return 0
            }
        }
    }

    private fun filter(
            organizationId: UUID, mappedData: Sequence<Map<String, UsageEventColumn>>
    ): Sequence<Map<String, UsageEventColumn>> {
        return mappedData.filter { mappedUsageEventCols ->
            val appName = mappedUsageEventCols[FQNS_TO_COLUMNS.getValue(FULL_NAME_FQN).name]?.value as String
            val eventDate = mappedUsageEventCols[FQNS_TO_COLUMNS.getValue(DATE_LOGGED_FQN).name]?.value as String
            val appPackageName = appName
            val dateLogged = getTruncatedDateTime(eventDate, organizationId)
            !scheduledTasksManager.systemAppPackageNames.contains(appPackageName) && dateLogged != null
        }
    }

    private fun mapToStorageModel(data: List<SetMultimap<UUID, Any>>): Sequence<Map<String, UsageEventColumn>> {
        return data.asSequence().map { usageEvent ->
            USAGE_EVENT_COLUMNS.associate { fqn ->
                val col = FQNS_TO_COLUMNS.getValue(fqn)
                val colIndex = getInsertUsageEventColumnIndex(col)
                val ptId = edmCacheManager.getPropertyTypeId(fqn)
                val value = usageEvent[ptId]?.iterator()?.next()
                col.name to UsageEventColumn(col, colIndex, value)
            }
        }
    }

    private fun writeToRedshift(
            hds: HikariDataSource,
            organizationId: UUID,
            studyId: UUID,
            participantId: String,
            data: Sequence<Map<String, UsageEventColumn>>
    ): Int {
        return hds.connection.use { connection ->
            //Create the temporary merge table
            val tempMergeTable = CHRONICLE_USAGE_EVENTS.createTempTable()
            try {
                connection.autoCommit = false

                connection.createStatement().use { stmt -> stmt.execute(tempMergeTable.createTableQuery()) }

                val wc = connection
                        .prepareStatement(getInsertIntoMergeUsageEventsTableSql(tempMergeTable.name)).use { ps ->
                            //Should only need to set these once for prepared statement.
                            ps.setString(1, organizationId.toString())
                            ps.setString(2, studyId.toString())
                            ps.setString(3, participantId)

                            data.forEach { usageEventCols ->
                                usageEventCols.values.forEach { usageEventCol ->

                                    val col = usageEventCol.col
                                    val colIndex = usageEventCol.colIndex
                                    val value = usageEventCol.value

                                    //Set insert value to null, if value was not provided.
                                    if (value == null) {
                                        ps.setObject(colIndex, null)
                                    } else {
                                        when (col.datatype) {
                                            PostgresDatatype.TEXT -> ps.setString(colIndex, value as String)
                                            PostgresDatatype.TIMESTAMPTZ -> ps.setObject(
                                                    colIndex,
                                                    OffsetDateTime.parse(value as String?)
                                            )
                                            PostgresDatatype.BIGINT -> ps.setLong(colIndex, value as Long)
                                            else -> ps.setObject(colIndex, value)
                                        }
                                    }
                                }
                                ps.addBatch()
                            }
                            ps.executeBatch().sum()
                        }

                connection.createStatement().use { stmt ->
                    stmt.execute(getDeleteTempTableEntriesSql(tempMergeTable.name))
                    stmt.executeUpdate(getAppendTembTableSql(tempMergeTable.name))
                }

                connection.commit()
                connection.autoCommit = true
                return@use wc
            } catch (ex: Exception) {
                logger.error("Unable to save data to redshift.", ex)
                connection.rollback()
                throw ex
            }
        }
    }

    private fun writeToPostgres(
            hds: HikariDataSource,
            organizationId: UUID,
            studyId: UUID,
            participantId: String,
            data: Sequence<Map<String, UsageEventColumn>>
    ): Int {
        return writeToRedshift(hds, organizationId, studyId, participantId, data)
    }


    companion object {
        private const val APP_USAGE_FREQUENCY = "appUsageFrequency"
    }
}


private data class UsageEventColumn(
        val col: PostgresColumnDefinition,
        val colIndex: Int,
        val value: Any?
)

private val USAGE_EVENT_COLUMNS = listOf(
        FULL_NAME_FQN,
        RECORD_TYPE_FQN,
        DATE_LOGGED_FQN,
        TIMEZONE_FQN,
        USER_FQN,
        TITLE_FQN
)

private val USAGE_STAT_COLUMNS = listOf(
        FULL_NAME_FQN,
        RECORD_TYPE_FQN,
        START_DATE_TIME_FQN,
        END_DATE_TIME_FQN,
        DURATION_FQN,
        DATE_LOGGED_FQN,
        TIMEZONE_FQN,
        TITLE_FQN,
)