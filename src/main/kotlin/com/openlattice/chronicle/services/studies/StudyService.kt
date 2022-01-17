package com.openlattice.chronicle.services.studies

import com.dataloom.mappers.ObjectMappers
import com.openlattice.chronicle.storage.StorageResolver
import com.fasterxml.jackson.databind.ObjectMapper
import com.geekbeast.configuration.postgres.PostgresFlavor
import com.openlattice.chronicle.auditing.AuditingComponent
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.HazelcastAuthorizationService
import com.openlattice.chronicle.authorization.SecurableObjectType
import com.openlattice.chronicle.authorization.principals.Principals
import com.openlattice.chronicle.authorization.reservations.AclKeyReservationService
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.ORGANIZATION_STUDIES
import com.openlattice.chronicle.study.Study
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.STUDIES
import com.openlattice.chronicle.storage.PostgresColumns
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.sql.Connection
import java.util.UUID

/**
 * @author Solomon Tang <solomon@openlattice.com>
 */
@Service
class StudyService(
    private val storageResolver: StorageResolver,
    private val aclKeyReservationService: AclKeyReservationService,
    private val idGenerationService: HazelcastIdGenerationService,
    private val authorizationService: AuthorizationManager,
    override val auditingManager: AuditingManager
) : StudyManager, AuditingComponent {
    private val mapper = ObjectMappers.newJsonMapper()

    companion object {
        private val logger = LoggerFactory.getLogger(StudyService::class.java)
        private val objectMapper = ObjectMapper()
        private val STUDY_COLUMNS = listOf(
            PostgresColumns.STUDY_ID,
            PostgresColumns.TITLE,
            PostgresColumns.DESCRIPTION,
            PostgresColumns.LAT,
            PostgresColumns.LON,
            PostgresColumns.STUDY_GROUP,
            PostgresColumns.STUDY_VERSION,
            PostgresColumns.SETTINGS
        ).joinToString(",") { it.name }

        private val ORG_STUDIES_COLS = listOf(
            PostgresColumns.ORGANIZATION_ID,
            PostgresColumns.STUDY_ID,
            PostgresColumns.USER_ID,
            PostgresColumns.SETTINGS
        ).joinToString(",") { it.name }

        /**
         * 1. study id
         * 2. title
         * 3. description
         * 4. lat
         * 5. lon
         * 6. study group
         * 7. study version
         * 8. settings
         */
        private val INSERT_STUDY_SQL = """
            INSERT INTO ${STUDIES.name} ($STUDY_COLUMNS) VALUES (?,?,?,?,?,?,?,?::jsonb)
        """.trimIndent()

        /**
         * 1. organization_id
         * 2. study_id
         * 3. user_id
         * 4. settings
         */
        private val INSERT_ORG_STUDIES_SQL =
            """
                INSERT IN ${ORGANIZATION_STUDIES.name} (${ORG_STUDIES_COLS}) VALUES (?,?,?,?)
            """.trimIndent()
    }

    override fun createStudy(study: Study): UUID {
        study.id = idGenerationService.getNextId()
        
            val (flavor, hds) = storageResolver.resolve(study.id)
            check(flavor == PostgresFlavor.VANILLA) { "Only vanilla postgres is supported for studies." }
            val connection = hds.connection
            connection.autoCommit = false
            try {
                insertStudy(connection, study)
                authorizationService.createSecurableObject(
                    connection = connection,
                    aclKey = AclKey(study.id),
                    principal = Principals.getCurrentUser(),
                    objectType = SecurableObjectType.Study
                )
                connection.commit()
            } catch (ex: Exception) {
                logger.error("Failed to create study $study.", ex)
                connection.rollback()
                throw ex
            } finally {
                connection.autoCommit = true
            }
        return study.id
    }

    private fun insertStudy(connection: Connection, study: Study): Int {
        val ps = connection.prepareStatement(INSERT_STUDY_SQL)
        ps.setObject(1, study.id)
        ps.setObject(2, study.title)
        ps.setObject(3, study.description)
        ps.setObject(4, study.lat)
        ps.setObject(5, study.lon)
        ps.setObject(6, study.group)
        ps.setObject(7, study.version)
        ps.setString(8, mapper.writeValueAsString(study.settings))
        return ps.executeUpdate()
    }
}