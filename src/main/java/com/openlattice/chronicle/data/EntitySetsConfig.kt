package com.openlattice.chronicle.data

import com.openlattice.chronicle.constants.CollectionTemplateTypeName
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
interface EntitySetsConfig {
    // core entity set ids
    val hasEntitySetId: UUID
    val metadataEntitySetId: UUID
    val notificationEntitySetId: UUID
    val partOfEntitySetId: UUID
    val participantEntitySetId: UUID
    val participatedInEntitySetId: UUID
    val studiesEntitySetId: UUID

    // data collection
    val appDataEntitySetId:UUID
    val appDictionaryEntitySetId: UUID
    val deviceEntitySetId: UUID
    val preprocessedDataEntitySetId: UUID
    val recordedByEntitySetId: UUID
    val usedByEntitySetId: UUID
    val userAppsEntitySetId: UUID

    // surveys
    val addressesEntitySetId: UUID
    val answersEntitySetId: UUID
    val questionsEntitySetId: UUID
    val registeredForEntitySetId: UUID
    val respondsWithEntitySetId: UUID
    val submissionEntitySetId: UUID
    val surveysEntitySetId: UUID
    val timeRangeEntitySetId: UUID

    fun getAllEntitySetIds(): Set<UUID>
}
