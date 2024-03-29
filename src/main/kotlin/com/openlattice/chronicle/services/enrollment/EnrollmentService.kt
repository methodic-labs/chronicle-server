package com.openlattice.chronicle.services.enrollment

import com.geekbeast.controllers.exceptions.ResourceNotFoundException
import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.openlattice.chronicle.data.ParticipationStatus
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.participants.Participant
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.services.candidates.CandidateManager
import com.openlattice.chronicle.sources.AndroidDevice
import com.openlattice.chronicle.sources.IOSDevice
import com.openlattice.chronicle.sources.SourceDevice
import com.openlattice.chronicle.sources.SourceDeviceType
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.DEVICES
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.STUDY_PARTICIPANTS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.DEVICE_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.DEVICE_TOKEN
import com.openlattice.chronicle.storage.PostgresColumns.Companion.DEVICE_TYPE
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PARTICIPANT_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PARTICIPATION_STATUS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SOURCE_DEVICE_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.util.ChronicleServerUtil
import org.slf4j.LoggerFactory
import org.springframework.security.access.AccessDeniedException
import org.springframework.stereotype.Service
import java.sql.Connection
import java.util.*
import javax.xml.transform.Source

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Service
class EnrollmentService(
    private val storageResolver: StorageResolver,
    private val idGenerationService: HazelcastIdGenerationService,
    private val candidateManager: CandidateManager,
) : EnrollmentManager {

    companion object {
        private val logger = LoggerFactory.getLogger(EnrollmentService::class.java)
        private val DEVICES_COLS = DEVICES.columns.joinToString(",") { it.name }
        private val STUDY_PARTICIPANT_COLS = STUDY_PARTICIPANTS.columns.joinToString(",") { it.name }
        private val mapper = ObjectMappers.newJsonMapper()

        /**
         * 1. study id
         * 2. device id
         * 3. participant id
         * 4. device type
         * 5. source device id
         * 6. source device
         * 7. device token
         */
        private val INSERT_DEVICE = """
            INSERT INTO ${DEVICES.name} ($DEVICES_COLS) VALUES (?,?,?,?,?,?::jsonb,?) 
            ON CONFLICT (${STUDY_ID.name},${PARTICIPANT_ID.name}, ${SOURCE_DEVICE_ID.name}) DO UPDATE SET ${DEVICE_TOKEN.name} = EXCLUDED.${DEVICE_TOKEN.name}             
        """

        /**
         * 1. study id
         * 2. participant id
         * 3. source device id
         */
        private val GET_DEVICE_ID = """
            SELECT ${DEVICE_ID.name} FROM ${DEVICES.name} 
                WHERE ${STUDY_ID.name} = ? AND ${PARTICIPANT_ID.name} = ? AND ${SOURCE_DEVICE_ID.name} = ? 
        """.trimIndent()

        /**
         * Retrieves the distinct enrolled device types
         * 1. study id
         * 2. participant id
         */
        private val GET_DEVICE_TYPES = """
            SELECT DISTINCT ${STUDY_ID.name}, ${PARTICIPANT_ID.name}, array_agg(distinct ${DEVICE_TYPE.name}) as ${DEVICE_TYPE.name}
            FROM ${DEVICES.name}
            WHERE ${STUDY_ID.name} = ANY(?) AND ${PARTICIPANT_ID.name} = ANY(?)
            GROUP BY ${STUDY_ID.name}, ${PARTICIPANT_ID.name}
        """.trimIndent()

        /**
         * 1. study id
         * 2. participant id
         * 3. source device id
         */
        private val COUNT_DEVICE_ID = """
            SELECT count(*) FROM ${DEVICES.name} 
                WHERE ${STUDY_ID.name} = ? AND ${PARTICIPANT_ID.name} = ? AND ${SOURCE_DEVICE_ID.name} = ? 
        """.trimIndent()

        private val COUNT_STUDY_PARTICIPANTS = """
            SELECT count(*) FROM ${STUDY_PARTICIPANTS.name} WHERE ${STUDY_ID.name} = ? AND ${PARTICIPANT_ID.name} = ?
        """.trimIndent()

        /**
         * 1. study id
         * 2. participant id
         * 3. candidate id
         * 4. participation status
         */
        private val INSERT_PARTICIPANT = """
            INSERT INTO ${STUDY_PARTICIPANTS.name} ($STUDY_PARTICIPANT_COLS) VALUES (?,?,?,?)
        """.trimIndent()

        /**
         * 1. study id
         * 2. participant id
         */
        private val GET_PARTICIPANT = """
            SELECT * FROM ${STUDY_PARTICIPANTS.name} WHERE ${STUDY_ID.name} = ? AND ${PARTICIPANT_ID.name} = ?
        """.trimIndent()

        /**
         * 1. study id
         * 2. participant id
         */
        private val GET_PARTICIPATION_STATUS = """
            SELECT ${PARTICIPATION_STATUS.name} FROM ${STUDY_PARTICIPANTS.name} WHERE ${STUDY_ID.name} = ? AND ${PARTICIPANT_ID.name} = ?
        """.trimIndent()

    }

    override fun registerDevice(
        studyId: UUID,
        participantId: String,
        sourceDeviceId: String,
        sourceDevice: SourceDevice,
    ): UUID {
        logger.info(
            "attempting to register data source" + ChronicleServerUtil.STUDY_PARTICIPANT_DATASOURCE,
            studyId,
            participantId,
            sourceDeviceId
        )

        if (!isKnownParticipant(studyId, participantId)) {
            logger.error(
                "unknown participant, unable to register datasource" + ChronicleServerUtil.STUDY_PARTICIPANT_DATASOURCE,
                studyId,
                participantId,
                sourceDeviceId
            )
            throw AccessDeniedException("unknown participant, unable to register datasource")
        }


        return when (sourceDevice) {
            is AndroidDevice -> registerDeviceOrGetId(
                studyId,
                participantId,
                SourceDeviceType.Android,
                sourceDeviceId,
                sourceDevice,
                sourceDevice.fcmRegistrationToken
            )

            is IOSDevice -> registerDeviceOrGetId(
                studyId,
                participantId,
                SourceDeviceType.Ios,
                sourceDeviceId,
                sourceDevice,
                sourceDevice.apnDeviceToken
            )

            else -> throw UnsupportedOperationException("${sourceDevice.javaClass.name} is not a supported datasource.")
        }
    }

    private fun registerDeviceOrGetId(
        studyId: UUID,
        participantId: String,
        deviceType: SourceDeviceType,
        sourceDeviceId: String,
        sourceDevice: SourceDevice,
        deviceToken: String,
    ): UUID {
        val hds = storageResolver.getPlatformStorage()
        val deviceId = idGenerationService.getNextId()
        val insertCount = hds.connection.use { connection ->
            connection.prepareStatement(INSERT_DEVICE).use { ps ->
                ps.setObject(1, studyId)
                ps.setObject(2, deviceId)
                ps.setString(3, participantId)
                ps.setString(4, deviceType.name)
                ps.setString(5, sourceDeviceId)
                ps.setString(6, mapper.writeValueAsString(sourceDevice))
                ps.setString(7, deviceToken)
                ps.executeUpdate()
            }
        }

        if (insertCount > 0) {
            return deviceId
        }
        logger.info(
            "Datasource for ${ChronicleServerUtil.STUDY_PARTICIPANT_DATASOURCE} is already registered. Returning",
            studyId,
            participantId,
            sourceDeviceId
        )
        return getDeviceId(studyId, participantId, sourceDeviceId)
    }

    override fun getDeviceId(studyId: UUID, participantId: String, sourceDeviceId: String): UUID {
        val hds = storageResolver.getPlatformStorage()

        return hds.connection.use { connection ->
            connection.prepareStatement(GET_DEVICE_ID).use { ps ->
                ps.setObject(1, studyId)
                ps.setString(2, participantId)
                ps.setString(3, sourceDeviceId)
                ps.executeQuery().use { rs ->
                    if (rs.next()) {
                        ResultSetAdapters.deviceId(rs)
                    } else {
                        throw ResourceNotFoundException("Unable to find device for study=$studyId, participant=$participantId, sourceDeviceId=$sourceDeviceId")
                    }
                }
            }
        }
    }

    override fun registerParticipant(
        connection: Connection,
        studyId: UUID,
        participantId: String,
        candidateId: UUID,
        participationStatus: ParticipationStatus,
    ) {
        connection.prepareStatement(INSERT_PARTICIPANT).use { ps ->
            ps.setObject(1, studyId)
            ps.setString(2, participantId)
            ps.setObject(3, candidateId)
            ps.setString(4, participationStatus.name)
            ps.executeUpdate()
        }
    }

    override fun isKnownDatasource(
        studyId: UUID,
        participantId: String,
        sourceDeviceId: String,
    ): Boolean {
        val hds = storageResolver.getPlatformStorage()

        return hds.connection.use { connection ->
            connection.prepareStatement(COUNT_DEVICE_ID).use { ps ->
                ps.setObject(1, studyId)
                ps.setString(2, participantId)
                ps.setString(3, sourceDeviceId)
                ps.executeQuery().use { rs ->
                    check(rs.next()) { "No count returned for study=$studyId, participant=$participantId, sourceDeviceId=$sourceDeviceId" }
                    ResultSetAdapters.count(rs) > 0 //could also check equal to one, but unique index exists in db
                }
            }
        }
    }

    override fun isKnownParticipant(studyId: UUID, participantId: String): Boolean {
        val hds = storageResolver.getPlatformStorage()

        return hds.connection.use { connection ->
            connection.prepareStatement(COUNT_STUDY_PARTICIPANTS).use { ps ->
                ps.setObject(1, studyId)
                ps.setString(2, participantId)
                ps.executeQuery().use { rs ->
                    check(rs.next()) { "No count returned for study=$studyId, participant=$participantId" }
                    ResultSetAdapters.count(rs) > 0 //could also check equal to one, but unique index exists in db
                }
            }
        }
    }

    override fun getParticipant(studyId: UUID, participantId: String): Participant {
        val hds = storageResolver.getPlatformStorage()

        val (participationStatus, candidateId) = hds.connection.use { connection ->
            connection.prepareStatement(GET_PARTICIPANT).use { ps ->
                ps.setObject(1, studyId)
                ps.setString(2, participantId)
                ps.executeQuery().use { rs ->
                    check(rs.next()) { "No row returned for study=$studyId, participant=$participantId" }
                    ResultSetAdapters.participantStatus(rs) to ResultSetAdapters.candidateId(rs)
                }
            }
        }
        return Participant(participantId, candidateManager.getCandidate(candidateId), participationStatus)
    }

    override fun getParticipationStatus(
        studyId: UUID,
        participantId: String,
    ): ParticipationStatus {
        logger.info(
            "getting participation status" + ChronicleServerUtil.STUDY_PARTICIPANT, studyId,
            participantId
        )

        val hds = storageResolver.getPlatformStorage()

        return hds.connection.use { connection ->
            connection.prepareStatement(GET_PARTICIPATION_STATUS).use { ps ->
                ps.setObject(1, studyId)
                ps.setString(2, participantId)
                ps.executeQuery().use {
                    check(it.next()) { "No row returned for study=$studyId, participant=$participantId" }
                    ResultSetAdapters.participantStatus(it)
                }
            }
        }
    }

    override fun getStudyParticipantIds(studyId: UUID): Set<String> {
        TODO("Not yet implemented")
    }

    override fun getStudyParticipants(studyId: UUID): Set<Participant> {
        TODO("Not yet implemented")
    }

    override fun studyExists(studyId: UUID): Boolean {
        TODO("Not yet implemented")
    }

    override fun getOrganizationIdForStudy(studyId: UUID): UUID {
        TODO("Not yet implemented")
    }


}
