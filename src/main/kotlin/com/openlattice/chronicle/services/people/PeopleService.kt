package com.openlattice.chronicle.services.people

import com.openlattice.chronicle.auditing.AuditingComponent
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.people.Person
import org.apache.commons.lang3.NotImplementedException
import java.sql.Connection

class PeopleService(
    override val auditingManager: AuditingManager
) : PeopleManager, AuditingComponent {

    override fun createPerson(connection: Connection, person: Person) {
        throw NotImplementedException("coming soon")
    }
}
