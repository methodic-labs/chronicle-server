package com.openlattice.chronicle.services.delete;

import com.openlattice.chronicle.data.ChronicleDeleteType;

import java.util.UUID;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public interface DataDeletionManager {

    void deleteParticipantAndAllNeighbors(
            UUID organizationId,
            UUID studyId,
            String participantId,
            ChronicleDeleteType chronicleDeleteType,
            String token );

    void deleteStudyAndAllNeighbors(
            UUID organizationId,
            UUID studyId,
            ChronicleDeleteType chronicleDeleteType,
            String token
    );
}
