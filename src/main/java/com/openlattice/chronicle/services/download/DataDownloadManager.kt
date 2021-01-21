package com.openlattice.chronicle.services.download

import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
interface DataDownloadManager {
    fun getAllParticipantData(
            organizationId: UUID?,
            studyId: UUID?,
            participantEntityId: UUID?,
            token: String?): Iterable<Map<String, Set<Any>>>

    fun getAllPreprocessedParticipantData(
            organizationId: UUID?,
            studyId: UUID?,
            participantEntityId: UUID?,
            token: String?): Iterable<Map<String, Set<Any>>>

    fun getAllParticipantAppsUsageData(
            organizationId: UUID?,
            studyId: UUID?,
            participantEntityId: UUID?,
            token: String?): Iterable<Map<String, Set<Any>>>
}