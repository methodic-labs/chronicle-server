package com.openlattice.chronicle.mapstores.storage

import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.mapstores.AbstractBasePostgresMapstore
import com.geekbeast.postgres.mapstores.AbstractPostgresMapstore2
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.geekbeast.postgres.streams.StatementHolderSupplier
import com.geekbeast.rhizome.mapstores.TestableSelfRegisteringMapStore
import com.hazelcast.config.MapConfig
import com.hazelcast.config.MapStoreConfig
import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.services.studies.StudyService.Companion.GET_STUDIES_SQL
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.ORGANIZATION_STUDIES
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.STUDIES
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.STUDY_LIMITS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import com.openlattice.chronicle.study.Study
import com.openlattice.chronicle.study.StudyLimits
import com.openlattice.chronicle.util.tests.TestDataFactory
import com.zaxxer.hikari.HikariDataSource
import org.springframework.stereotype.Component
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class StudyLimitsMapstore(val hds: HikariDataSource) :
    AbstractBasePostgresMapstore<UUID, StudyLimits>(HazelcastMap.STUDY_LIMITS, STUDY_LIMITS, hds) {
    private val mapper = ObjectMappers.newJsonMapper()
    override fun bind(ps: PreparedStatement, key: UUID, value: StudyLimits) {
        var index = bind(ps, key, 1)
        ps.setObject(index, value.participantLimit)
        ps.setObject(index + 1, mapper.writeValueAsString(value.studyDuration))
        ps.setObject(index + 2, mapper.writeValueAsString(value.dataRetentionDuration))
        ps.setArray(index + 3, PostgresArrays.createTextArray(ps.connection, value.features.map { it.name }))
    }

    override fun mapToKey(rs: ResultSet): UUID = rs.getObject(STUDY_ID.name, UUID::class.java)

    override fun bind(ps: PreparedStatement, key: UUID, offset: Int): Int {
        ps.setObject(1, key)
        return 2
    }

    override fun mapToValue(rs: ResultSet): StudyLimits = ResultSetAdapters.studyLimits( rs )

    override fun generateTestKey(): UUID = UUID.randomUUID()


    override fun generateTestValue(): StudyLimits = StudyLimits()

}
