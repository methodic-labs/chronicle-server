package com.openlattice.chronicle.data

import com.openlattice.chronicle.constants.CollectionTemplateTypeName
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
// ALl chronicle entity set ids
class ChronicleAppConfig(
        private val coreEntitySets: Map<CollectionTemplateTypeName, UUID>,
        private val dataCollectionEntitySets: Map<CollectionTemplateTypeName, UUID>,
        private val questionnairesEntitySets: Map<CollectionTemplateTypeName, UUID>) : EntitySetsConfig {

    // core entity set ids
    override val hasEntitySetId = coreEntitySets.getValue(CollectionTemplateTypeName.HAS)
    override val metadataEntitySetId = coreEntitySets.getValue(CollectionTemplateTypeName.METADATA)
    override val notificationEntitySetId = coreEntitySets.getValue(CollectionTemplateTypeName.NOTIFICATION)
    override val partOfEntitySetId = coreEntitySets.getValue(CollectionTemplateTypeName.PART_OF)
    override val participantEntitySetId = coreEntitySets.getValue(CollectionTemplateTypeName.PARTICIPANTS)
    override val participatedInEntitySetId = coreEntitySets.getValue(CollectionTemplateTypeName.PARTICIPATED_IN)
    override val studiesEntitySetId = coreEntitySets.getValue(CollectionTemplateTypeName.STUDIES)

    // data collection
    override val appDataEntitySetId = dataCollectionEntitySets.getValue(CollectionTemplateTypeName.APPDATA)
    override val appDictionaryEntitySetId = dataCollectionEntitySets.getValue(CollectionTemplateTypeName.APP_DICTIONARY)
    override val deviceEntitySetId = dataCollectionEntitySets.getValue(CollectionTemplateTypeName.DEVICE)
    override val preprocessedDataEntitySetId = dataCollectionEntitySets.getValue(CollectionTemplateTypeName.PREPROCESSED_DATA)
    override val recordedByEntitySetId = dataCollectionEntitySets.getValue(CollectionTemplateTypeName.RECORDED_BY)
    override val usedByEntitySetId = dataCollectionEntitySets.getValue(CollectionTemplateTypeName.USED_BY)
    override val userAppsEntitySetId = dataCollectionEntitySets.getValue(CollectionTemplateTypeName.USER_APPS)

    // surveys
    override val addressesEntitySetId = questionnairesEntitySets.getValue(CollectionTemplateTypeName.ADDRESSES)
    override val answersEntitySetId = questionnairesEntitySets.getValue(CollectionTemplateTypeName.ANSWER)
    override val questionsEntitySetId = questionnairesEntitySets.getValue(CollectionTemplateTypeName.QUESTION)
    override val registeredForEntitySetId = questionnairesEntitySets.getValue(CollectionTemplateTypeName.REGISTERED_FOR)
    override val respondsWithEntitySetId = questionnairesEntitySets.getValue(CollectionTemplateTypeName.RESPONDS_WITH)
    override val submissionEntitySetId = questionnairesEntitySets.getValue(CollectionTemplateTypeName.SUBMISSION)
    override val surveysEntitySetId = questionnairesEntitySets.getValue(CollectionTemplateTypeName.SURVEY)
    override val timeRangeEntitySetId = questionnairesEntitySets.getValue(CollectionTemplateTypeName.TIME_RANGE)

    override fun getAllEntitySetIds(): Set<UUID> {
        return ( coreEntitySets + dataCollectionEntitySets + questionnairesEntitySets ).values.toSet()
    }
}


