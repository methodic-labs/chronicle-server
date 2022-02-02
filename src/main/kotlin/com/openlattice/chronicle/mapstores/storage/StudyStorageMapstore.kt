package com.openlattice.chronicle.mapstores.storage

import com.geekbeast.postgres.mapstores.AbstractBasePostgresMapstore
import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.storage.ChroniclePostgresTables
import com.openlattice.chronicle.study.Study
import com.openlattice.chronicle.util.TestDataFactory
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class StudyStorageMapstore(hds: HikariDataSource) :
    AbstractBasePostgresMapstore<UUID, Study>(HazelcastMap.STUDIES, ChroniclePostgresTables.STUDIES, hds) {

    override fun mapToKey(rs: ResultSet): UUID = ResultSetAdapters.studyId(rs)

    override fun bind(ps: PreparedStatement, key: UUID, value: Study) {
        ps.setObject(1, key)

    }

    override fun bind(ps: PreparedStatement, key: UUID, offset: Int): Int {
        ps.setObject(offset, key)
        return offset + 1
    }

    override fun mapToValue(rs: ResultSet): Study = ResultSetAdapters.study(rs)

    override fun generateTestKey(): UUID = UUID.randomUUID()
    override fun generateTestValue(): Study = TestDataFactory.study()
}
