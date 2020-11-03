package com.openlattice.chronicle.services.upload;

import com.google.common.collect.SetMultimap;

import java.util.List;
import java.util.UUID;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public interface AppDataUploadManager {

    Integer upload(
            UUID organizationId,
            UUID studyId,
            String participantId,
            String datasourceId,
            List<SetMultimap<UUID, Object>> data
    );
}
