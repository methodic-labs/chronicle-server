package com.openlattice.chronicle.data

import com.openlattice.chronicle.constants.EdmConstants.*
import com.openlattice.chronicle.util.ChronicleServerUtil
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class LegacyChronicleAppConfig(entitySetIds: Map<String, UUID>, val studyId: UUID) {

    private val participantEntitySetName = ChronicleServerUtil.getParticipantEntitySetName(studyId)

    // entity set ids
    val addressesEntitySetId = entitySetIds.getValue(ADDRESSES_ES)
    val answerEntitySetId = entitySetIds.getValue(ANSWERS_ES)
    val appDataEntitySetId = entitySetIds.getValue(DATA_ES)
    val appDictionaryEntitySetId = entitySetIds.getValue(APPS_DICTIONARY_ES)
    val deviceEntitySetId = entitySetIds.getValue(DEVICES_ES)
    val hasEntitySetId = entitySetIds.getValue(HAS_ES)
    val metadataEntitySeId = entitySetIds.getValue(METADATA_ES)
    val notificationEntitySetId = entitySetIds.getValue(NOTIFICATION_ES)
    val partOfEntitySetId = entitySetIds.getValue(PART_OF_ES)
    val participantEntitySetId = entitySetIds.getValue(participantEntitySetName)
    val participatedInEntitySetId = entitySetIds.getValue(PARTICIPATED_IN_ES)
    val preprocessedDataEntitySetId = entitySetIds.getValue(PREPROCESSED_DATA_ES)
    val questionnaireEntitySetId = entitySetIds.getValue(QUESTIONNAIRE_ES)
    val registeredForEntitySetId = entitySetIds.getValue(REGISTERED_FOR_ES)
    val respondsWithEntitySetId = entitySetIds.getValue(RESPONDS_WITH_ES)
    val studiesEntitySetId = entitySetIds.getValue(STUDY_ES)
    val submissionEntitySetId = entitySetIds.getValue(SUBMISSION_ES)
    val timeRangeEntitySetId = entitySetIds.getValue(TIMERANGE_ES)
    val usedByEntitySetId = entitySetIds.getValue(USED_BY_ES)
    val userAppsEntitySetId = entitySetIds.getValue(USER_APPS_ES)
}

