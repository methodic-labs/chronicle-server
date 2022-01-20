package com.openlattice.chronicle.auditing

interface AuditingManager {

    fun recordEvents(events: List<AuditableEvent>): Int

}