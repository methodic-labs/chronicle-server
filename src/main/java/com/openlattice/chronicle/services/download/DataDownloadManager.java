package com.openlattice.chronicle.services.download;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public interface DataDownloadManager {
    Iterable<Map<String, Set<Object>>> getAllParticipantData(
            UUID organizationId,
            UUID studyId,
            UUID participantEntityId,
            String token );

    Iterable<Map<String, Set<Object>>> getAllPreprocessedParticipantData(
            UUID organizationId,
            UUID studyId,
            UUID participantEntityId,
            String token );

    Iterable<Map<String, Set<Object>>> getAllParticipantAppsUsageData(
            UUID organizationId,
            UUID studyId,
            UUID participantEntityId,
            String token );

}
