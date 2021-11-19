package com.openlattice.chronicle.data

import com.openlattice.chronicle.constants.CollectionTemplateTypeName
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
// ALl chronicle entity set ids
class ChronicleAppConfig(
        coreEntitySets: Map<CollectionTemplateTypeName, UUID>,
        dataCollectionEntitySets: Map<CollectionTemplateTypeName, UUID>,
        questionnairesEntitySets: Map<CollectionTemplateTypeName, UUID>) {

    // core entity set ids
    val hasEntitySetId = coreEntitySets.getValue(CollectionTemplateTypeName.HAS)
    val metadataEntitySetId = coreEntitySets.getValue(CollectionTemplateTypeName.METADATA)
    val notificationEntitySetId = coreEntitySets.getValue(CollectionTemplateTypeName.NOTIFICATION)
    val partOfEntitySetId = coreEntitySets.getValue(CollectionTemplateTypeName.PART_OF)
    val participantEntitySetId = coreEntitySets.getValue(CollectionTemplateTypeName.PARTICIPANTS)
    val participatedInEntitySeId = coreEntitySets.getValue(CollectionTemplateTypeName.PARTICIPATED_IN)
    val studiesEntitySetId = coreEntitySets.getValue(CollectionTemplateTypeName.STUDIES)

    // data collection
    val appDataEntitySetId = dataCollectionEntitySets[CollectionTemplateTypeName.APPDATA]
    val appDictionaryEntitySetId = dataCollectionEntitySets[CollectionTemplateTypeName.APP_DICTIONARY]
    val deviceEntitySetId = dataCollectionEntitySets[CollectionTemplateTypeName.DEVICE]
    val preprocessedDataEntitySetId = dataCollectionEntitySets[CollectionTemplateTypeName.PREPROCESSED_DATA]
    val recordedByEntitySetId = dataCollectionEntitySets[CollectionTemplateTypeName.RECORDED_BY]
    val usedByEntitySetId = dataCollectionEntitySets[CollectionTemplateTypeName.USED_BY]
    val userAppsEntitySetId = dataCollectionEntitySets[CollectionTemplateTypeName.USER_APPS]

    // surveys
    val addressesEntitySetId = questionnairesEntitySets[CollectionTemplateTypeName.ADDRESSES]
    val answersEntitySetId = questionnairesEntitySets[CollectionTemplateTypeName.ANSWER]
    val questionsEntitySetId = questionnairesEntitySets[CollectionTemplateTypeName.QUESTION]
    val registeredForEntitySetId = questionnairesEntitySets[CollectionTemplateTypeName.REGISTERED_FOR]
    val respondsWithEntitySetId = questionnairesEntitySets[CollectionTemplateTypeName.RESPONDS_WITH]
    val submissionEntitySetId = questionnairesEntitySets[CollectionTemplateTypeName.SUBMISSION]
    val surveysEntitySetId = questionnairesEntitySets[CollectionTemplateTypeName.SURVEY]
    val timeRangeEntitySetId = questionnairesEntitySets[CollectionTemplateTypeName.TIME_RANGE]

    fun getAllEntitySetIds(): Set<UUID> {
        return setOf(
                hasEntitySetId,
                metadataEntitySetId,
                notificationEntitySetId,
                partOfEntitySetId,
                participantEntitySetId,
                participatedInEntitySeId,
                appDataEntitySetId,
                appDictionaryEntitySetId,
                deviceEntitySetId,
                preprocessedDataEntitySetId,
                recordedByEntitySetId,
                usedByEntitySetId,
                userAppsEntitySetId,
                addressesEntitySetId,
                answersEntitySetId,
                questionsEntitySetId,
                registeredForEntitySetId,
                respondsWithEntitySetId,
                submissionEntitySetId,
                surveysEntitySetId,
                timeRangeEntitySetId
        ).filterNotNull().toSet()
    }
}


