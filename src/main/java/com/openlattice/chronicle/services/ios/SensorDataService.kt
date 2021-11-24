package com.openlattice.chronicle.services.ios

import com.google.common.base.Stopwatch
import com.google.common.collect.ArrayListMultimap
import com.google.common.collect.ListMultimap
import com.openlattice.ApiHelpers
import com.openlattice.chronicle.constants.EdmConstants.DATASOURCE_FQN
import com.openlattice.chronicle.constants.EdmConstants.DATE_LOGGED_FQN
import com.openlattice.chronicle.constants.EdmConstants.END_DATE_TIME_FQN
import com.openlattice.chronicle.constants.EdmConstants.NAME_FQN
import com.openlattice.chronicle.constants.EdmConstants.OL_ID_FQN
import com.openlattice.chronicle.constants.EdmConstants.RECORDED_DATE_TIME_FQN
import com.openlattice.chronicle.constants.EdmConstants.START_DATE_TIME_FQN
import com.openlattice.chronicle.constants.EdmConstants.TIMEZONE_FQN
import com.openlattice.chronicle.data.EntityUpdateDefinition
import com.openlattice.chronicle.data.SensorDataSample
import com.openlattice.chronicle.services.ApiCacheManager
import com.openlattice.chronicle.services.edm.EdmCacheManager
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.services.entitysets.EntitySetIdsManager
import com.openlattice.client.ApiClient
import com.openlattice.data.DataApi
import com.openlattice.data.DataAssociation
import com.openlattice.data.DataEdgeKey
import com.openlattice.data.DataGraph
import com.openlattice.data.DataIntegrationApi
import com.openlattice.data.EntityDataKey
import com.openlattice.data.EntityKey
import com.openlattice.data.PropertyUpdateType
import com.openlattice.data.UpdateType
import com.openlattice.data.integration.Association
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.TimeUnit

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class SensorDataService(
        val apiCacheManager: ApiCacheManager,
        val edmCacheManager: EdmCacheManager,
        val entitySetIdsManager: EntitySetIdsManager,
        val enrollmentManager: EnrollmentManager) : SensorDataManager {

    companion object {
        private const val DATA_SOURCE = "iPhone"
    }

    private val logger = LoggerFactory.getLogger(SensorDataService::class.java)

    private var loggerStringTemplate: String = ""

    // property type ids
    private val namePTID = edmCacheManager.getPropertyTypeId(NAME_FQN)
    private val idPTID = edmCacheManager.getPropertyTypeId(OL_ID_FQN)
    private val dateLoggedPTID = edmCacheManager.getPropertyTypeId(DATE_LOGGED_FQN)
    private val startDateTimePTID = edmCacheManager.getPropertyTypeId(START_DATE_TIME_FQN)
    private val endDateTImePTID = edmCacheManager.getPropertyTypeId(END_DATE_TIME_FQN)
    private val timezonePTID = edmCacheManager.getPropertyTypeId(TIMEZONE_FQN)
    private val recordedDatePTID = edmCacheManager.getPropertyTypeId(RECORDED_DATE_TIME_FQN)
    private val datasourcePTID = edmCacheManager.getPropertyTypeId(DATASOURCE_FQN)

    private fun getSensorDataAssociation(
            timestamp: OffsetDateTime,
            sampleId: UUID,
            index: Int,
            partOfESID: UUID,
            recordedByESID: UUID,
            sensorDataESID: UUID,
            deviceEntityDataKey: EntityDataKey,
            participantEntityDataKey: EntityDataKey,
            sensorEntityDataKey: EntityDataKey): ListMultimap<UUID, DataAssociation> {

        val associations: ListMultimap<UUID, DataAssociation> = ArrayListMultimap.create()

        val recordedByEntity = mapOf(dateLoggedPTID to setOf(timestamp))
        val partOfEntity = mapOf(idPTID to setOf(sampleId))

        //sensorData -> recorded by -> device
        associations.put(recordedByESID, DataAssociation(
                sensorDataESID,
                Optional.of(index),
                Optional.empty(),
                deviceEntityDataKey.entitySetId,
                Optional.empty(),
                Optional.of(deviceEntityDataKey.entityKeyId),
                recordedByEntity
        ))

        //sensorData -> recorded by -> participant
        associations.put(recordedByESID, DataAssociation(
                sensorDataESID,
                Optional.of(index),
                Optional.empty(),
                participantEntityDataKey.entitySetId,
                Optional.empty(),
                Optional.of(participantEntityDataKey.entityKeyId),
                recordedByEntity
        ))

        //sensorData -> part of -> sensor
        associations.put(partOfESID, DataAssociation(
                sensorDataESID,
                Optional.of(index),
                Optional.empty(),
                sensorEntityDataKey.entitySetId,
                Optional.empty(),
                Optional.of(sensorEntityDataKey.entityKeyId),
                partOfEntity
        ))

        return associations
    }

    private fun getSensorEntityData(
            sensorName: String
    ): Map<UUID, Set<Any>> {
        return mapOf(namePTID to setOf(sensorName))
    }

    private fun getEntityKey(
            entityData: Map<UUID, Set<Any>>,
            entitySetId: UUID
    ): EntityKey {
        return EntityKey(entitySetId, ApiHelpers.generateDefaultEntityId(entityData.keys, entityData))
    }

    private fun getParticipantEntityKey(
            participantEntityDataKey: EntityDataKey
    ): EntityKey {
        return EntityKey(participantEntityDataKey.entitySetId, participantEntityDataKey.entityKeyId.toString())
    }

    private fun generateEntityKeyIdsForEntityKeys(
            entityKeys: Set<EntityKey>,
            participantEntityDataKey: EntityDataKey,
            dataIntegrationApi: DataIntegrationApi
    ): Map<EntityKey, UUID> {
        val participantEntityKey = getParticipantEntityKey(participantEntityDataKey)

        val entityKeysHashset = linkedSetOf<EntityKey>()
        entityKeysHashset.addAll(entityKeys)

        val ids = dataIntegrationApi.getEntityKeyIds(entityKeysHashset)

        return entityKeysHashset.zip(ids).toMap() + mapOf(participantEntityKey to participantEntityDataKey.entityKeyId)
    }

    private fun getMetadataEntitiesAndAssociations(
            dataIntegrationApi: DataIntegrationApi,
            dataApi: DataApi,
            dates: SortedSet<OffsetDateTime>,
            metadataEntitySetId: UUID,
            hasEntitySetId: UUID,
            participantEntityDataKey: EntityDataKey
    ): Pair<Set<EntityUpdateDefinition>, Set<Association>> {

        val entities: MutableSet<EntityUpdateDefinition> = mutableSetOf()
        val associations: MutableSet<Association> = mutableSetOf()

        var firstDateTime = dates.first()
        var lastDateTime = dates.last()
        val uniqueDates = dates.map { it.truncatedTo(ChronoUnit.DAYS) }.toSet()

        val metadataEntityData: MutableMap<UUID, Set<Any>> = mutableMapOf(
                idPTID to setOf(participantEntityDataKey.entityKeyId),
                datasourcePTID to setOf(DATA_SOURCE)
        )
        // unique for each participant
        val metadataEntityKey = getEntityKey(metadataEntityData, metadataEntitySetId)

        // get current metadata
        val metadataEntityKeyId = dataIntegrationApi.getEntityKeyIds(setOf(metadataEntityKey)).first()
        val currentMetadata: Map<FullQualifiedName, Set<Any>>
        try {
            currentMetadata = dataApi.getEntity(metadataEntitySetId, metadataEntityKeyId)
            val currentFirstDate = OffsetDateTime.parse(currentMetadata[START_DATE_TIME_FQN]?.first().toString())
            val currentLastDate = OffsetDateTime.parse(currentMetadata[END_DATE_TIME_FQN]?.first().toString())

            firstDateTime = if (currentFirstDate != null) minOf(currentFirstDate, firstDateTime) else firstDateTime
            lastDateTime = if (currentLastDate != null) maxOf(currentLastDate, lastDateTime) else lastDateTime
        } catch (e: Exception) {
            logger.error("error retrieving participant metadata: $loggerStringTemplate")
        }

        entities += setOf(
                EntityUpdateDefinition(metadataEntityKey, metadataEntityData + mapOf(recordedDatePTID to uniqueDates), UpdateType.Merge),
                EntityUpdateDefinition(metadataEntityKey, mapOf(startDateTimePTID to setOf(firstDateTime), endDateTImePTID to setOf(lastDateTime)), UpdateType.PartialReplace),
        )

        val hasEntityData = mapOf(
                idPTID to setOf(participantEntityDataKey.entityKeyId)
        )
        val hasEntityKey = getEntityKey(hasEntityData, hasEntitySetId)
        entities.add(EntityUpdateDefinition(hasEntityKey, hasEntityData, UpdateType.Merge))

        // association: participant -> has -> metadata
        val participantEntityKey = EntityKey(participantEntityDataKey.entitySetId, participantEntityDataKey.entityKeyId.toString())
        associations.add(Association(hasEntityKey, participantEntityKey, metadataEntityKey))

        return Pair(entities, associations)
    }

    private fun getSensorEntities(
            sensorNames: Set<String>,
            sensorEntitySetId: UUID,
    ): Set<EntityUpdateDefinition> {

        val sensorEntityData = sensorNames.map { getSensorEntityData(it) }
        val sensorEntityKeys = sensorEntityData.associateBy { getEntityKey(it, sensorEntitySetId) }

        return sensorEntityKeys.entries.map { EntityUpdateDefinition(it.key, it.value, UpdateType.Merge) }.toSet()
    }

    override fun uploadData(organizationId: UUID, studyId: UUID, participantId: String, deviceId: String, data: List<SensorDataSample>) {

        val timer = Stopwatch.createStarted()
        this.loggerStringTemplate = "orgId - $organizationId, studyId - $studyId, participantId - $participantId, deviceId - $deviceId"

        val apiClient = apiCacheManager.intApiClientCache.get(ApiClient::class.java)
        val dataIntegrationApi = apiClient.dataIntegrationApi
        val dataApi = apiClient.dataApi

        val coreAppConfig = entitySetIdsManager.getChronicleAppConfig(organizationId)
        val dataCollectionAppConfig = entitySetIdsManager.getChronicleDataCollectionAppConfig(organizationId)

        val sensorDataEntitySetId = dataCollectionAppConfig.sensorDataEntitySetId
        val sensorEntitySetId = dataCollectionAppConfig.sensorEntitySetId
        val recordedByEntitySetId = dataCollectionAppConfig.recordedByEntitySetId
        val deviceEntitySetId = dataCollectionAppConfig.deviceEntitySetId
        val participantEntitySetId = coreAppConfig.participantEntitySetId
        val partOfEntitySetId = coreAppConfig.partOfEntitySetId
        val metadataEntitySetId = coreAppConfig.metadataEntitySetId
        val hasEntitySetId = coreAppConfig.hasEntitySetId

        val deviceEKID = enrollmentManager.getDeviceEntityKeyId(organizationId, studyId, participantId, deviceId)
        checkNotNull(deviceEKID) { "Could not upload sensor data because deviceEntityKeyId is invalid: $loggerStringTemplate" }

        val participantEKID = enrollmentManager.getParticipantEntityKeyId(organizationId, studyId, participantId)
        checkNotNull(participantEKID) { "Could not upload data sensor because participant is invalid: $loggerStringTemplate" }

        val deviceEntityDataKey = EntityDataKey(deviceEntitySetId, deviceEKID)
        val participantEntityDataKey = EntityDataKey(participantEntitySetId, participantEntitySetId)

        val entities: ListMultimap<UUID, Map<UUID, Set<Any>>> = ArrayListMultimap.create()
        val associations: ListMultimap<UUID, DataAssociation> = ArrayListMultimap.create()

        val sensorNames = data.map { it.sensorName }.toSet()
        val dates = data.map { it.dateRecorded }.toSet()

        try {
            val (metadataEntities, metadataAssociations) = getMetadataEntitiesAndAssociations(dataIntegrationApi, dataApi, dates.toSortedSet(), metadataEntitySetId, hasEntitySetId, participantEntityDataKey)
            val sensorEntities = getSensorEntities(sensorNames, sensorEntitySetId)

            val entityUpdates = sensorEntities + metadataEntities
            val allEntityKeys = entityUpdates.map { it.entityKey }.toSet()

            // get entity key ids
            val entityKeyByEntityKeyId: Map<EntityKey, UUID> = generateEntityKeyIdsForEntityKeys(allEntityKeys, participantEntityDataKey, dataIntegrationApi)

            val entityUpdateByEntityKeyId: Map<EntityUpdateDefinition, UUID> = entityUpdates.associateWith { entityKeyByEntityKeyId.getValue(it.entityKey) }
            val entityUpdatesByEntitySetId: Map<UUID, List<EntityUpdateDefinition>> = entityUpdates.groupBy { it.entityKey.entitySetId }

            entityUpdatesByEntitySetId.forEach { (entitySetId, updates) ->
                run {
                    updates.forEach {
                        val entity = mapOf(entityUpdateByEntityKeyId.getValue(it) to it.details)
                        dataApi.updateEntitiesInEntitySet(entitySetId, entity, it.updateType, PropertyUpdateType.Versioned)
                    }
                }
            }

            val timeStamp = OffsetDateTime.now()

            var index = 0
            data.forEach { sample ->

                val sensorEntityData = getSensorEntityData(sample.sensorName)
                val sensorEntityKey = getEntityKey(sensorEntityData, sensorEntitySetId)
                val sensorEntityKeyId = entityKeyByEntityKeyId.getValue(sensorEntityKey)

                val sensorEntityDataKey = EntityDataKey(sensorEntitySetId, sensorEntityKeyId)

                sample.data.forEach { entity ->
                    val sensorDataEntity = entity.mapValues { entry -> setOf(entry.value) } + mapOf(
                            idPTID to setOf(sample.id),
                            dateLoggedPTID to setOf(sample.dateRecorded),
                            startDateTimePTID to setOf(sample.startDate),
                            endDateTImePTID to setOf(sample.endDate),
                            timezonePTID to setOf(sample.timezone)
                    )

                    entities.put(sensorDataEntitySetId, sensorDataEntity)
                    associations.putAll(getSensorDataAssociation(timeStamp, sample.id, index++, partOfEntitySetId, recordedByEntitySetId, sensorDataEntitySetId, deviceEntityDataKey, participantEntityDataKey, sensorEntityDataKey))
                }
            }

            val dataGraph = DataGraph(entities, associations)
            dataApi.createEntityAndAssociationData(dataGraph)

            // create edges
            val dataEdgeKeys = metadataAssociations.map {
                val src = EntityDataKey(it.src.entitySetId, entityKeyByEntityKeyId.getValue(it.src))
                val edge = EntityDataKey(it.key.entitySetId, entityKeyByEntityKeyId.getValue(it.key))
                val dst = EntityDataKey(it.dst.entitySetId, entityKeyByEntityKeyId.getValue(it.dst))

                DataEdgeKey(src, dst, edge)
            }.toSet()

            dataApi.createEdges(dataEdgeKeys)

            timer.stop()

            logger.info("logging ${data.size} sensor data samples took ${timer.elapsed(TimeUnit.SECONDS)} seconds. $loggerStringTemplate")

        } catch (e: Exception) {
            logger.error("An error occurred while attempting to upload sensor data: $loggerStringTemplate", e)
        }
    }
}
