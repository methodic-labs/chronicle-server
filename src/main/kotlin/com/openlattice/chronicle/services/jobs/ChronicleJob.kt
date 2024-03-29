package com.openlattice.chronicle.services.jobs

import com.fasterxml.jackson.annotation.JsonCreator
import com.geekbeast.rhizome.jobs.JobStatus
import com.openlattice.chronicle.authorization.Principal
import com.openlattice.chronicle.authorization.principals.Principals
import com.openlattice.chronicle.ids.IdConstants
import java.time.OffsetDateTime
import java.util.UUID

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
data class ChronicleJob @JsonCreator constructor(
    var id: UUID = IdConstants.UNINITIALIZED.id,
    val securablePrincipalId: UUID = Principals.getCurrentSecurablePrincipal().id,
    val principal: Principal = Principals.getCurrentUser(),
    val createdAt: OffsetDateTime = OffsetDateTime.now(),
    var updatedAt: OffsetDateTime = OffsetDateTime.now(),
    var completedAt: OffsetDateTime = OffsetDateTime.MAX,
    var status: JobStatus = JobStatus.PENDING,
    val contact: String = "",
    val definition: ChronicleJobDefinition = EmptyJobDefinition(),
    val message: String = "",
    var deletedRows: Long = 0L //TODO: Make this a flexible state variable
)