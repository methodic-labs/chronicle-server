package com.openlattice.chronicle.services.delete;

import com.openlattice.chronicle.data.DeleteType;

import java.util.UUID;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public interface DataDeletionManager {

    void deleteParticipantAndAllNeighbors(
            UUID organizationId,
            UUID studyId,
            String participantId,
            DeleteType deleteType,
            String token );

    void deleteStudyAndAllNeighbors(
            UUID organizationId,
            UUID studyId,
            DeleteType deleteType,
            String token
    );
}
