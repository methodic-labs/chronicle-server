package com.openlattice.chronicle.services;

import com.google.common.collect.SetMultimap;
import java.util.List;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public interface ChronicleService {
    //  TODO: add in throws exception!
    Integer logData(
            UUID studyId,
            String participantId,
            String datasourceId,
            List<SetMultimap<UUID, Object>> data );

    UUID registerDatasource( UUID studyId, String participantId, String datasourceId );

    boolean isKnownDatasource( UUID studyId, String participantId, String datasourceId );

    boolean isKnownParticipant( UUID studyId, String participantId );
}
