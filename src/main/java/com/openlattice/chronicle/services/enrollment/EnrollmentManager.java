package com.openlattice.chronicle.services.enrollment;

import com.google.common.base.Optional;
import com.openlattice.chronicle.data.ParticipationStatus;
import com.openlattice.chronicle.sources.Datasource;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public interface EnrollmentManager {

    UUID registerDatasource(
            UUID organizationId,
            UUID studyId,
            String participantId,
            String datasourceId,
            Optional<Datasource> datasource );

    UUID getDeviceEntityKeyId( UUID organizationId, UUID studyId, String participantId, String datasourceId );

    boolean isKnownDatasource( UUID organizationId, UUID studyId, String participantId, String datasourceId );

    boolean isKnownParticipant( UUID organizationId, UUID studyId, String participantId );

    Map<FullQualifiedName, Set<Object>> getParticipantEntity(
            UUID organizationId,
            UUID studyId,
            UUID participantEntityId );

    ParticipationStatus getParticipationStatus( UUID organizationId, UUID studyId, String participantId );

    boolean isNotificationsEnabled( UUID organizationId, UUID studyId );

    UUID getParticipantEntityKeyId( UUID organizationId, UUID studyId, String participantId );

    UUID getStudyEntityKeyId( UUID organizationId, UUID studyId );
}
