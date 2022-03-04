package com.openlattice.chronicle.jobs

import com.fasterxml.jackson.annotation.JsonTypeInfo
import java.util.*

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@type")
interface ChronicleParticipantJobDefinition : ChronicleJobDefinition {
    val studyId: UUID
    val participantIds: Collection<String>
}