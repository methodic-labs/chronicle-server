package com.openlattice.chronicle.services.surveys;

import com.openlattice.chronicle.data.ChronicleAppsUsageDetails;
import com.openlattice.chronicle.data.ChronicleQuestionnaire;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public interface SurveysManager {
    void submitAppUsageSurvey(
            UUID organizationId,
            UUID studyId,
            String participantId,
            Map<UUID, Map<FullQualifiedName, Set<Object>>> associationDetails );

    ChronicleQuestionnaire getQuestionnaire( UUID organizationId, UUID studyId, UUID questionnaireEKID );

    Map<UUID, Map<FullQualifiedName, Set<Object>>> getStudyQuestionnaires( UUID organizationId, UUID studyId );

    void submitQuestionnaire(
            UUID organizationId,
            UUID studyId,
            String participantId,
            Map<UUID, Map<FullQualifiedName, Set<Object>>> questionnaireResponses
    );

    List<ChronicleAppsUsageDetails> getParticipantAppsUsageData (UUID organizationId, UUID studyId, String participantId, String date);

}
