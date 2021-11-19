package com.openlattice.chronicle.services.ios

import com.google.common.base.Stopwatch
import com.google.common.collect.ArrayListMultimap
import com.google.common.collect.ListMultimap
import com.openlattice.ApiHelpers
import com.openlattice.chronicle.constants.EdmConstants.*
import com.openlattice.chronicle.data.SensorDataSample
import com.openlattice.chronicle.services.ApiCacheManager
import com.openlattice.chronicle.services.ScheduledTasksManager
import com.openlattice.chronicle.services.edm.EdmCacheManager
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.services.entitysets.EntitySetIdsManager
import com.openlattice.client.ApiClient
import com.openlattice.data.*
import org.slf4j.LoggerFactory
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.TimeUnit

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class SensorDataService(
        val apiCacheManager: ApiCacheManager,
        val edmCacheManager: EdmCacheManager,
        val entitySetIdsManager: EntitySetIdsManager,
        val scheduledTasksManager: ScheduledTasksManager,
        val enrollmentManager: EnrollmentManager) : SensorDataManager {

    private val logger = LoggerFactory.getLogger(SensorDataService::class.java)

    // property type ids
    private val namePTID = edmCacheManager.getPropertyTypeId(NAME_FQN)
    private val valuesPTID = edmCacheManager.getPropertyTypeId(VALUES_FQN)
    private val idPTID = edmCacheManager.getPropertyTypeId(OL_ID_FQN)
    private val dateLoggedPTID = edmCacheManager.getPropertyTypeId(DATE_LOGGED_FQN)
    private val startDateTimePTID = edmCacheManager.getPropertyTypeId(START_DATE_TIME_FQN)
    private val endDateTImePTID = edmCacheManager.getPropertyTypeId(END_DATE_TIME_FQN)
    private val timezonePTID = edmCacheManager.getPropertyTypeId(TIMEZONE_FQN)


    // entity set id to use

    private fun getAssociations(timestamp: OffsetDateTime, sampleId: UUID, index: Int, partOfESID: UUID, recordedByESID: UUID, sensorDataESID: UUID, deviceEntityDataKey: EntityDataKey, participantEntityDataKey: EntityDataKey, sensorEntityDataKey: EntityDataKey): ListMultimap<UUID, DataAssociation> {

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

    override fun uploadData(organizationId: UUID, studyId: UUID, participantId: String, deviceId: String, data: List<SensorDataSample>) {

        val timer = Stopwatch.createStarted()
        val enrollmentDetailsStringTemplate = "orgId - $organizationId, studyId - $studyId, participantId - $participantId, deviceId - $deviceId"

        val apiClient = apiCacheManager.intApiClientCache.get(ApiClient::class.java)
        val integrationApi = apiClient.dataIntegrationApi
        val dataApi = apiClient.dataApi

        val coreAppConfig = entitySetIdsManager.getChronicleAppConfig(organizationId)
        val dataCollectionAppConfig = entitySetIdsManager.getChronicleDataCollectionAppConfig(organizationId)

        val sensorDataEntitySetId = dataCollectionAppConfig.sensorDataEntitySetId
        val sensorEntitySetId = dataCollectionAppConfig.sensorEntitySetId
        val recordedByEntitySetId = dataCollectionAppConfig.recordedByEntitySetId
        val deviceEntitySetId = dataCollectionAppConfig.deviceEntitySetId
        val participantEntitySetId = coreAppConfig.participantEntitySetId
        val partOfEntitySetId = coreAppConfig.partOfEntitySetId

        val deviceEKID = enrollmentManager.getDeviceEntityKeyId(organizationId, studyId, participantId, deviceId)
        checkNotNull(deviceEKID) { "Could not upload sensor data because deviceEntityKeyId is invalid: $enrollmentDetailsStringTemplate" }

        val participantEKID = enrollmentManager.getParticipantEntityKeyId(organizationId, studyId, participantId)
        checkNotNull(participantEKID) { "Could not upload data sensor because participant is invalid: $enrollmentDetailsStringTemplate" }

        val deviceEntityDataKey = EntityDataKey(deviceEntitySetId, deviceEKID)
        val participantEntityDataKey = EntityDataKey(participantEntitySetId, participantEntitySetId)

        val entities: ListMultimap<UUID, Map<UUID, Set<Any>>> = ArrayListMultimap.create()
        val associations: ListMultimap<UUID, DataAssociation> = ArrayListMultimap.create()

        val sensors = data.map { it.sensorName }.toSet()

        val sensorNameToEntityData = sensors.associateWith { mapOf(namePTID to setOf(it)) }
        val sensorNameToEntityKey = sensorNameToEntityData.mapValues { EntityKey(sensorEntitySetId, ApiHelpers.generateDefaultEntityId(setOf(namePTID), it.value)) }

        val entityKeys = linkedSetOf<EntityKey>()
        entityKeys.addAll(sensorNameToEntityKey.values)

        val entityKeyIds = integrationApi.getEntityKeyIds(entityKeys)

        val entityKeyToEntityKeyId = entityKeys.toList().zip(entityKeyIds).toMap()
        val sensorNameToEntityKeyId = sensorNameToEntityKey.mapValues { entityKeyToEntityKeyId.getValue(it.value) }

        val entitiesToUpdate = sensorNameToEntityKeyId.entries.associate { (k, v) -> v to sensorNameToEntityData.getValue(k) }
        dataApi.updateEntitiesInEntitySet(sensorEntitySetId, entitiesToUpdate, UpdateType.PartialReplace, PropertyUpdateType.Versioned)

        // TODO: participant -> has -> metadata

        val timeStamp = OffsetDateTime.now()

        var index = 0
        data.forEach { sample ->

            val sampleId = sample.id
            val sensorEntityKeyId = sensorNameToEntityKeyId.getValue(sample.sensorName)
            val sensorEntityKey = EntityDataKey(sensorEntitySetId, sensorEntityKeyId)

            sample.data.forEach { entity ->
                val sensorDataEntity = entity.mapValues { entry -> setOf(entry.value) } + mapOf(
                        idPTID to setOf(sample.id),
                        dateLoggedPTID to setOf(sample.dateRecorded),
                        startDateTimePTID to setOf(sample.startDate),
                        endDateTImePTID to setOf(sample.endDate),
                        timezonePTID to setOf(sample.timezone)
                )

                entities.put(sensorDataEntitySetId, sensorDataEntity)
                associations.putAll(getAssociations(timeStamp, sampleId, index++, partOfEntitySetId, recordedByEntitySetId, sensorDataEntitySetId, deviceEntityDataKey, participantEntityDataKey, sensorEntityKey))
            }
        }


        val dataGraph = DataGraph(entities, associations)
        dataApi.createEntityAndAssociationData(dataGraph)

        timer.stop()

        logger.info("logging ${data.size} sensor data samples took ${timer.elapsed(TimeUnit.SECONDS)} seconds. $enrollmentDetailsStringTemplate")
    }
}