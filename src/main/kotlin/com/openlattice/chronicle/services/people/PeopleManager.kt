package com.openlattice.chronicle.services.people

import com.openlattice.chronicle.people.Person
import java.sql.Connection

interface PeopleManager {
    fun createPerson(connection: Connection, person: Person)
}
