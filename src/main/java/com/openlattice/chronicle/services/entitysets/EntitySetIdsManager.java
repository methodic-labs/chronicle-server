package com.openlattice.chronicle.services.entitysets;

import com.openlattice.chronicle.constants.*;
import com.openlattice.chronicle.data.ChronicleCoreAppConfig;
import com.openlattice.chronicle.data.ChronicleDataCollectionAppConfig;
import com.openlattice.chronicle.data.ChronicleSurveysAppConfig;

import java.util.Map;
import java.util.UUID;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public interface EntitySetIdsManager {
    Map<AppComponent, Map<UUID, Map<CollectionTemplateTypeName, UUID>>> getEntitySetIdsByOrgId();

    // entity set ids configs
    ChronicleDataCollectionAppConfig getChronicleDataCollectionAppConfig( UUID organizationId );

    ChronicleCoreAppConfig getChronicleAppConfig( UUID organizationId );

    ChronicleCoreAppConfig getChronicleAppConfig( UUID organizationId, String participantESName );

    ChronicleSurveysAppConfig getChronicleSurveysAppConfig( UUID organizationId );

    // legacy entity set ids configs
    ChronicleSurveysAppConfig getLegacyChronicleSurveysAppConfig();

    ChronicleDataCollectionAppConfig getLegacyChronicleDataCollectionAppConfig();

    ChronicleCoreAppConfig getLegacyChronicleAppConfig( String participantESName );

    ChronicleCoreAppConfig getLegacyChronicleAppConfig();

}
