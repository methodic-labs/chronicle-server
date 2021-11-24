package com.openlattice.chronicle.data

import com.openlattice.data.EntityKey
import com.openlattice.data.UpdateType
import com.openlattice.data.integration.Entity
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
data class EntityUpdateDefinition(
        val entityKey: EntityKey,
        val entityDetails: Map<UUID, Set<Any>>,
        val updateType: UpdateType
): Entity(entityKey, entityDetails)
