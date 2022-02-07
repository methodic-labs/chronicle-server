package com.openlattice.chronicle.services.download

import com.geekbeast.postgres.PostgresColumnDefinition
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.openlattice.chronicle.constants.OutputConstants
import com.openlattice.chronicle.constants.ParticipantDataType
import com.openlattice.chronicle.converters.PostgresDownloadWrapper
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APPLICATION_LABEL
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.APP_PACKAGE_NAME
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.INTERACTION_TYPE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMESTAMP
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.TIMEZONE
import com.openlattice.chronicle.storage.RedshiftColumns.Companion.USERNAME
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.CHRONICLE_USAGE_EVENTS
import com.openlattice.chronicle.storage.StorageResolver
import org.slf4j.LoggerFactory
import java.sql.ResultSet
import java.time.OffsetDateTime
import java.time.ZoneId
import java.util.*

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class DataDownloadService(private val storageResolver: StorageResolver) : DataDownloadManager {
    companion object {
        private val logger = LoggerFactory.getLogger(DataDownloadService::class.java)
        private val CHRONICLE_USAGE_EVENTS_DEFS = CHRONICLE_USAGE_EVENTS.columns.map { it.name }
        private val CHRONICLE_USAGE_EVENTS_COLS = CHRONICLE_USAGE_EVENTS_DEFS.joinToString(",")
        private val CHRONICLE_USAGE_EVENT_SQL = """
            SELECT $CHRONICLE_USAGE_EVENTS_COLS FROM ${CHRONICLE_USAGE_EVENTS.name} WHERE 
             ${STUDY_ID.name} = ? AND ${PARTICIPANT_ID.name} = ?
        """.trimIndent()

        fun associateString(rs: ResultSet, pcd: PostgresColumnDefinition) = pcd.name to rs.getString(pcd.name)
        fun associateOffsetDatetimeWithTimezone(
            rs: ResultSet,
            timezoneColumn: PostgresColumnDefinition,
            timestampColumn: PostgresColumnDefinition
        ): Pair<String, OffsetDateTime> {
            val zoneId = ZoneId.of( rs.getString( timezoneColumn.name ) ?: OutputConstants.DEFAULT_TIMEZONE )
            val odt = rs.getObject( timezoneColumn.name , OffsetDateTime::class.java )
            return timestampColumn.name to odt.toInstant().atZone(zoneId).toOffsetDateTime()
        }

        fun associateObject(rs: ResultSet, pcd: PostgresColumnDefinition, clazz: Class<*>) =
            pcd.name to rs.getObject(pcd.name, clazz)
    }

    private fun getParticipantDataHelper(
        studyId: UUID,
        participantId: String,
        dataType: ParticipantDataType
    ): Iterable<Map<String, Any>> {
        val (flavor, hds) = storageResolver.resolveAndGetFlavor(studyId)
        val pgIter = BasePostgresIterable<Map<String, Any>>(
            PreparedStatementHolderSupplier(
                hds,
                CHRONICLE_USAGE_EVENT_SQL,
                32768
            ) { ps ->
                ps.setString(1, studyId.toString())
                ps.setString(2, participantId)
            }) { rs ->
            mapOf(
                associateString(rs, ORGANIZATION_ID),
                associateString(rs, STUDY_ID),
                associateString(rs, PARTICIPANT_ID),
                associateString(rs, APP_PACKAGE_NAME),
                associateString(rs, INTERACTION_TYPE),
                associateOffsetDatetimeWithTimezone(rs, TIMEZONE, TIMESTAMP),
                associateString(rs, TIMEZONE),
                associateString(rs, USERNAME),
                associateString(rs, APPLICATION_LABEL)
            )
        }

        return PostgresDownloadWrapper(pgIter).withColumnAdvice(CHRONICLE_USAGE_EVENTS.columns.map { it.name })
    }

    override fun getParticipantData(
        studyId: UUID,
        participantId: String,
        dataType: ParticipantDataType,
        token: String
    ): Iterable<Map<String, Any>> {
        return getParticipantDataHelper(
            studyId,
            participantId,
            dataType
        )
    }


}