package com.openlattice.chronicle.mapstores.stats

import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.mapstores.AbstractBasePostgresMapstore
import com.hazelcast.config.EvictionConfig
import com.hazelcast.config.MapConfig
import com.hazelcast.config.MapStoreConfig
import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.openlattice.chronicle.participants.ParticipantStats
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.PARTICIPANT_STATS
import com.openlattice.chronicle.util.tests.TestDataFactory
import com.zaxxer.hikari.HikariDataSource
import org.apache.commons.lang3.RandomStringUtils
import org.springframework.stereotype.Service
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@getmethodic.com&gt;
 */
@Service
    class ParticipantStatsMapstore(hds: HikariDataSource) : AbstractBasePostgresMapstore<ParticipantKey, ParticipantStats>(
    HazelcastMap.PARTICIPANT_STATS,
    PARTICIPANT_STATS,
    hds
) {
    override fun getMapStoreConfig(): MapStoreConfig {
        return super.getMapStoreConfig()
            .setWriteDelaySeconds(5)
    }

    //TODO: Consider setting an eviction policy for this map store
    override fun getMapConfig(): MapConfig {
        return super.getMapConfig()
    }

    override fun bind(ps: PreparedStatement, key: ParticipantKey, value: ParticipantStats) {
        val offset = bind(ps, key)
//        PostgresColumns.STUDY_ID,
//        PostgresColumns.PARTICIPANT_ID,
//        PostgresColumns.ANDROID_LAST_PING,
//        PostgresColumns.ANDROID_FIRST_DATE,
//        PostgresColumns.ANDROID_LAST_DATE,
//        PostgresColumns.ANDROID_UNIQUE_DATES,
//        PostgresColumns.IOS_LAST_PING,
//        PostgresColumns.IOS_FIRST_DATE,
//        PostgresColumns.IOS_LAST_DATE,
//        PostgresColumns.IOS_UNIQUE_DATES,
//        PostgresColumns.TUD_FIRST_DATE,
//        PostgresColumns.TUD_LAST_DATE,
//        PostgresColumns.TUD_UNIQUE_DATES
        ps.setObject(offset + 1, value.androidLastPing)
        ps.setObject(offset + 2, value.androidFirstDate)
        ps.setObject(offset + 3, value.androidLastDate)
        ps.setArray(offset + 4, PostgresArrays.createDateArray(ps.connection, value.androidUniqueDates))
        ps.setObject(offset + 5, value.iosLastPing)
        ps.setObject(offset + 6, value.iosFirstDate)
        ps.setObject(offset + 7, value.iosLastDate)
        ps.setArray(offset + 8, PostgresArrays.createDateArray(ps.connection, value.iosUniqueDates))
        ps.setObject(offset + 9, value.tudFirstDate)
        ps.setObject(offset + 10, value.tudLastDate)
        ps.setArray(offset + 11, PostgresArrays.createDateArray(ps.connection, value.tudUniqueDates))

    }

    override fun bind(ps: PreparedStatement, key: ParticipantKey, offset: Int): Int {
        ps.setObject(offset, key.studyId)
        ps.setString(offset + 1, key.participantId)
        return offset + 2
    }

    override fun generateTestKey(): ParticipantKey =
        ParticipantKey(
            UUID.randomUUID(),
            RandomStringUtils.randomAlphanumeric(8)
        )

    override fun generateTestValue(): ParticipantStats = TestDataFactory.participantStats()

    override fun mapToKey(rs: ResultSet): ParticipantKey {
        return ResultSetAdapters.participantKey(rs)
    }

    override fun mapToValue(rs: ResultSet): ParticipantStats {
        return ResultSetAdapters.participantStats(rs)
    }
}