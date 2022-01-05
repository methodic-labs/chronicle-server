package com.openlattice.chronicle.authorization

import com.google.common.eventbus.EventBus
import com.hazelcast.core.EntryEvent
import com.hazelcast.map.listener.EntryAddedListener
import com.hazelcast.map.listener.EntryRemovedListener
import com.hazelcast.map.listener.EntryUpdatedListener

/**
 * Handles internal permission change events. This class sits far below of authorization layer, so should not be
 * responsible for handling audit related events.
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PermissionMapListener(private val eventBus: EventBus) : EntryAddedListener<AceKey, AceValue>,
                                                              EntryRemovedListener<AceKey, AceValue>,
                                                              EntryUpdatedListener<AceKey, AceValue> {
    override fun entryAdded(event: EntryEvent<AceKey, AceValue>) {
        if (isMaterializationEvent(event)) {
            postMaterializationEvent(event)
        }
    }

    override fun entryRemoved(event: EntryEvent<AceKey, AceValue>) {
        if (isMaterializationEvent(event)) {
            postMaterializationEvent(event)
        }
    }

    override fun entryUpdated(event: EntryEvent<AceKey, AceValue>) {
        if (isMaterializationEvent(event)) {
            postMaterializationEvent(event)
        }
    }

    private fun postMaterializationEvent(event: EntryEvent<AceKey, AceValue>) {

    }

    private fun isMaterializationEvent(event: EntryEvent<AceKey, AceValue>): Boolean {
        return false
    }

    override fun equals(other: Any?): Boolean {
        return other != null && other is PermissionMapListener
    }

    override fun hashCode(): Int {
        return eventBus.hashCode()
    }
}
