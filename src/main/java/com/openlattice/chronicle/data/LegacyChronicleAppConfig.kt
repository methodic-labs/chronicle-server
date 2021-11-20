package com.openlattice.chronicle.data

import com.openlattice.chronicle.constants.EdmConstants.*
import com.openlattice.chronicle.util.ChronicleServerUtil
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class LegacyChronicleAppConfig(entitySetIds: Map<String, UUID>, studyId: UUID?) : EntitySetsConfig {

    private val participantEntitySetName = if (studyId != null) ChronicleServerUtil.getParticipantEntitySetName(studyId) else null

    // entity set ids
    override val addressesEntitySetId = entitySetIds.getValue(ADDRESSES_ES)
    override val answersEntitySetId = entitySetIds.getValue(ANSWERS_ES)
    override val appDataEntitySetId = entitySetIds.getValue(DATA_ES)
    override val appDictionaryEntitySetId = entitySetIds.getValue(APPS_DICTIONARY_ES)
    override val deviceEntitySetId = entitySetIds.getValue(DEVICES_ES)
    override val hasEntitySetId = entitySetIds.getValue(HAS_ES)
    override val messagesEntitySetId: UUID
        get() = TODO("Not yet implemented")
    override val metadataEntitySetId = entitySetIds.getValue(METADATA_ES)
    override val notificationEntitySetId = entitySetIds.getValue(NOTIFICATION_ES)
    override val partOfEntitySetId = entitySetIds.getValue(PART_OF_ES)
    override val participantEntitySetId: UUID? = entitySetIds[participantEntitySetName]
    override val participatedInEntitySetId = entitySetIds.getValue(PARTICIPATED_IN_ES)
    override val sentToEntitySetId: UUID
        get() = TODO("Not yet implemented")
    override val preprocessedDataEntitySetId = entitySetIds.getValue(PREPROCESSED_DATA_ES)
    override val surveysEntitySetId = entitySetIds.getValue(QUESTIONNAIRE_ES)
    override val questionsEntitySetId = entitySetIds.getValue(QUESTIONS_ES)
    override val recordedByEntitySetId = entitySetIds.getValue(RECORDED_BY_ES)
    override val registeredForEntitySetId = entitySetIds.getValue(REGISTERED_FOR_ES)
    override val respondsWithEntitySetId = entitySetIds.getValue(RESPONDS_WITH_ES)
    override val studiesEntitySetId = entitySetIds.getValue(STUDY_ES)
    override val submissionEntitySetId = entitySetIds.getValue(SUBMISSION_ES)
    override val timeRangeEntitySetId = entitySetIds.getValue(TIMERANGE_ES)
    override val usedByEntitySetId = entitySetIds.getValue(USED_BY_ES)
    override val userAppsEntitySetId = entitySetIds.getValue(USER_APPS_ES)

    override fun getAllEntitySetIds(): Set<UUID> {
        TODO("Not yet implemented")
    }
}

