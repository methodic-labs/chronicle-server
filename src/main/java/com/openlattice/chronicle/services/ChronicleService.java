/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 */

package com.openlattice.chronicle.services;

import com.google.common.base.Optional;
import com.google.common.collect.SetMultimap;
import com.openlattice.chronicle.constants.ParticipationStatus;
import com.openlattice.chronicle.sources.Datasource;
import com.openlattice.search.SearchApi;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

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

    UUID getDatasourceEntityKeyId( String datasourceId );

    boolean isKnownDatasource( UUID studyId, String participantId, String datasourceId );

    boolean isKnownParticipant( UUID studyId, String participantId );

    Map<String, UUID> getPropertyTypeIds( Set<String> propertyTypeFqns );

    Iterable<SetMultimap<String, Object>> getAllParticipantData( UUID studyId, UUID participantEntityId );

    Map<FullQualifiedName, Set<Object>> getParticipantEntity( UUID studyId, UUID participantEntityId );

    UUID getParticipantEntityKeyId( String participantId, UUID participantsEntitySetId, SearchApi searchApi );

    ParticipationStatus getParticipationStatus( UUID studyId, String participantId );
}
