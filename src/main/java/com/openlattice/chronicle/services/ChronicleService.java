package com.openlattice.chronicle.services;

import com.google.common.base.Optional;
import com.google.common.collect.SetMultimap;
import com.openlattice.chronicle.sources.Datasource;

import java.util.List;
import java.util.Map;
import java.util.Set;
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

    UUID registerDatasource( UUID studyId, String participantId, String datasourceId, Optional<Datasource> datasource );

    boolean isKnownDatasource( UUID studyId, String participantId, String datasourceId );

    boolean isKnownParticipant( UUID studyId, String participantId );

    Map<String, UUID> getPropertyTypeIds( Set<String> propertyTypeFqns );
}
