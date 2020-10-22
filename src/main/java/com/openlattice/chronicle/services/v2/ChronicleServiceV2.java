package com.openlattice.chronicle.services.v2;

import com.google.common.base.Optional;
import com.google.common.collect.SetMultimap;
import com.openlattice.chronicle.data.ChronicleAppsUsageDetails;
import com.openlattice.chronicle.data.ChronicleQuestionnaire;
import com.openlattice.chronicle.data.DeleteType;
import com.openlattice.chronicle.data.ParticipationStatus;
import com.openlattice.chronicle.sources.Datasource;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public interface ChronicleServiceV2 {
    Integer logData(
            UUID studyId,
            String participantId,
            String datasourceId,
            List<SetMultimap<UUID, Object>> data );

    Integer logData(
            UUID organizationId,
            UUID studyId,
            String participantId,
            String datasourceId,
            List<SetMultimap<UUID, Object>> data
    );

    UUID registerDatasource( UUID organizationId, UUID studyId, String participantId, String datasourceId, Optional<Datasource> datasource );

    UUID getDeviceEntityKeyId( UUID organizationId, UUID studyId, String participantId, String datasourceId );

    boolean isKnownDatasource( UUID organizationId, UUID studyId, String participantId, String datasourceId );

    boolean isKnownParticipant( UUID organizationId, UUID studyId, String participantId );

    void deleteParticipantAndAllNeighbors( UUID organizationId, UUID studyId, String participantId, DeleteType deleteType, String token );

    void deleteStudyAndAllNeighbors( UUID organizationId, UUID studyId, DeleteType deleteType, String token );

    Map<String, UUID> getPropertyTypeIds( Set<String> propertyTypeFqns );

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

    Map<FullQualifiedName, Set<Object>> getParticipantEntity( UUID organizationId, UUID studyId, UUID participantEntityId );

    ParticipationStatus getParticipationStatus( UUID organizationId, UUID studyId, String participantId );

    List<ChronicleAppsUsageDetails> getParticipantAppsUsageData(
            UUID organizationId,
            UUID studyId,
            String participantId,
            String date );

    Integer updateAppsUsageAssociationData(
            UUID organizationId,
            UUID studyId,
            String participantId,
            Map<UUID, Map<FullQualifiedName, Set<Object>>> associationDetails );

    boolean isNotificationsEnabled( UUID organizationId, UUID studyId );

    ChronicleQuestionnaire getQuestionnaire( UUID organizationId, UUID studyId, UUID questionnaireEKID );

    Map<UUID, Map<FullQualifiedName, Set<Object>>> getStudyQuestionnaires( UUID organizationId, UUID studyId );

    void submitQuestionnaire( UUID organizationId, UUID studyId, String participantId, Map<UUID, Map<FullQualifiedName, Set<Object>>> questionnaireResponses );
}
