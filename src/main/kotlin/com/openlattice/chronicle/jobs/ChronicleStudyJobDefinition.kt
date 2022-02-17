package com.openlattice.chronicle.jobs

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.geekbeast.rhizome.jobs.JobStatus
import com.openlattice.chronicle.authorization.Principal
import com.openlattice.chronicle.ids.IdConstants
import java.time.OffsetDateTime
import java.util.*

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@type")
interface ChronicleStudyJobDefinition : ChronicleJobDefinition {
    val studyId: UUID
}