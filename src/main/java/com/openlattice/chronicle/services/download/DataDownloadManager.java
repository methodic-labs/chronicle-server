package com.openlattice.chronicle.services.download;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public interface DataDownloadManager {
    ParticipantDataIterable getAllParticipantData(
            UUID organizationId,
            UUID studyId,
            UUID participantEntityId,
            String token );

    ParticipantDataIterable getAllPreprocessedParticipantData(
            UUID organizationId,
            UUID studyId,
            UUID participantEntityId,
            String token );

    ParticipantDataIterable getAllParticipantAppsUsageData(
            UUID organizationId,
            UUID studyId,
            UUID participantEntityId,
            String token );

}
