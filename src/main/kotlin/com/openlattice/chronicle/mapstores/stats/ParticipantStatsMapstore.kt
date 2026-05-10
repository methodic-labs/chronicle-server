package com.openlattice.chronicle.mapstores.stats

import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.PostgresColumnDefinition
import com.geekbeast.postgres.mapstores.AbstractBasePostgresMapstore
import com.google.common.collect.ImmutableList
import com.hazelcast.config.EvictionConfig
import com.hazelcast.config.MapConfig
import com.hazelcast.config.MapStoreConfig
import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.openlattice.chronicle.participants.ParticipantStats
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.PARTICIPANT_STATS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ANDROID_UNIQUE_DATES
import com.openlattice.chronicle.storage.PostgresColumns.Companion.IOS_UNIQUE_DATES
import com.openlattice.chronicle.storage.PostgresColumns.Companion.TUD_UNIQUE_DATES
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

    /**
     * The unique-dates arrays must merge via set-union on conflict so concurrent writes from
     * ingestion-time mergers and the periodic reconciliation SQL can't clobber each other's dates.
     */
    override fun initUnionColumns(): List<PostgresColumnDefinition> {
        return ImmutableList.of(ANDROID_UNIQUE_DATES, IOS_UNIQUE_DATES, TUD_UNIQUE_DATES)
    }

    override fun bind(ps: PreparedStatement, key: ParticipantKey, value: ParticipantStats) {
        var offset = bind(ps, key)

        // INSERT VALUES half — bind every value column.
        ps.setObject(offset++, value.androidLastPing)
        ps.setObject(offset++, value.androidFirstDate)
        ps.setObject(offset++, value.androidLastDate)
        ps.setArray(offset++, PostgresArrays.createDateArray(ps.connection, value.androidUniqueDates))
        ps.setObject(offset++, value.iosLastPing)
        ps.setObject(offset++, value.iosFirstDate)
        ps.setObject(offset++, value.iosLastDate)
        ps.setArray(offset++, PostgresArrays.createDateArray(ps.connection, value.iosUniqueDates))
        ps.setObject(offset++, value.tudFirstDate)
        ps.setObject(offset++, value.tudLastDate)
        ps.setArray(offset++, PostgresArrays.createDateArray(ps.connection, value.tudUniqueDates))

        // ON CONFLICT DO UPDATE SET half — skip the *_unique_dates columns (they're declared as
        // unionColumns() so the SET clause merges via array set-union with no parameter binding).
        ps.setObject(offset++, value.androidLastPing)
        ps.setObject(offset++, value.androidFirstDate)
        ps.setObject(offset++, value.androidLastDate)
        ps.setObject(offset++, value.iosLastPing)
        ps.setObject(offset++, value.iosFirstDate)
        ps.setObject(offset++, value.iosLastDate)
        ps.setObject(offset++, value.tudFirstDate)
        ps.setObject(offset++, value.tudLastDate)
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