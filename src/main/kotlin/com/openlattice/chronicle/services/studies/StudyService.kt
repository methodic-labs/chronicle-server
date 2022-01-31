package com.openlattice.chronicle.services.studies

import com.geekbeast.mappers.mappers.ObjectMappers
import com.openlattice.chronicle.storage.StorageResolver
import com.geekbeast.configuration.postgres.PostgresFlavor
import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.openlattice.chronicle.auditing.AuditingComponent
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.SecurableObjectType
import com.openlattice.chronicle.authorization.principals.Principals
import com.openlattice.chronicle.authorization.reservations.AclKeyReservationService
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.ORGANIZATION_STUDIES
import com.openlattice.chronicle.study.Study
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.STUDIES
import com.openlattice.chronicle.storage.PostgresColumns
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ORGANIZATION_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ORGANIZATION_IDS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.STUDY_ID
import com.openlattice.chronicle.study.StudyUpdate
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.sql.Connection
import java.time.OffsetDateTime
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
    companion object {
        private val logger = LoggerFactory.getLogger(StudyService::class.java)
        private val mapper = ObjectMappers.newJsonMapper()
        private val STUDY_COLUMNS = listOf(
            PostgresColumns.STUDY_ID,
            PostgresColumns.TITLE,
            PostgresColumns.DESCRIPTION,
            PostgresColumns.LAT,
            PostgresColumns.LON,
            PostgresColumns.STUDY_GROUP,
            PostgresColumns.STUDY_VERSION,
            PostgresColumns.CONTACT,
            PostgresColumns.SETTINGS
        ).joinToString(",") { it.name }

        private val UPDATE_STUDY_COLUMNS_LIST = listOf(
            PostgresColumns.TITLE,
            PostgresColumns.DESCRIPTION,
            PostgresColumns.UPDATED_AT,
            PostgresColumns.STARTED_AT,
            PostgresColumns.ENDED_AT,
            PostgresColumns.LAT,
            PostgresColumns.LON,
            PostgresColumns.STUDY_GROUP,
            PostgresColumns.STUDY_VERSION,
            PostgresColumns.CONTACT,
            PostgresColumns.SETTINGS
        )

        private val UPDATE_STUDY_COLUMNS = UPDATE_STUDY_COLUMNS_LIST.joinToString(",") { it.name }

        private val COALESCED_STUDY_COLUMNS = UPDATE_STUDY_COLUMNS_LIST
            .joinToString(",") {
            "coalesce(?${if(it.equals(PostgresColumns.SETTINGS)) "::jsonb" else ""}, ${it.name})"
        }

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
         * 8. contact
         * 9. settings
         */
        private val INSERT_STUDY_SQL = """
            INSERT INTO ${STUDIES.name} ($STUDY_COLUMNS) VALUES (?,?,?,?,?,?,?,?,?::jsonb)
        """.trimIndent()

        /**
         * 1. organization_id
         * 2. study_id
         * 3. user_id
         * 4. settings
         */
        private val INSERT_ORG_STUDIES_SQL =
            """
                INSERT INTO ${ORGANIZATION_STUDIES.name} (${ORG_STUDIES_COLS}) VALUES (?,?,?,?::jsonb)
            """.trimIndent()

        private val GET_STUDIES_SQL = """
            SELECT * FROM ${STUDIES.name} 
            LEFT JOIN (
                SELECT ${STUDY_ID.name}, array_agg(${ORGANIZATION_ID.name}) as ${ORGANIZATION_IDS.name} 
                    FROM ${ORGANIZATION_STUDIES.name}
                    GROUP BY ${STUDY_ID.name}
                ) as org_studies 
            USING (${STUDY_ID.name}) 
            WHERE ${STUDY_ID.name} = ANY(?)
        """.trimIndent()

        /**
         * 1. title,
         * 2. description,
         * 3. updated_at,
         * 4. started_at,
         * 5. ended_at,
         * 6. lat,
         * 7. lon,
         * 8. study_group,
         * 9. study_version,
         * 10. contact,
         * 11. settings
         * 12. study_id
         */

        private val UPDATE_STUDY_SQL = """
            UPDATE ${STUDIES.name}
            SET (${UPDATE_STUDY_COLUMNS}) = (${COALESCED_STUDY_COLUMNS})
            WHERE ${STUDY_ID.name} = ?
        """.trimIndent()
    }

    override fun createStudy(connection: Connection, study: Study) {
        insertStudy(connection, study)
        insertOrgStudy(connection, study)
        authorizationService.createSecurableObject(
            connection = connection,
            aclKey = AclKey(study.id),
            principal = Principals.getCurrentUser(),
            objectType = SecurableObjectType.Study
        )
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
        ps.setObject(8, study.contact)
        ps.setString(9, mapper.writeValueAsString(study.settings))
        return ps.executeUpdate()
    }

    private fun insertOrgStudy(connection: Connection, study: Study): Int {
        val ps = connection.prepareStatement(INSERT_ORG_STUDIES_SQL)
        study.organizationIds.forEach { organizationId ->
            var params = 1
            ps.setObject(params++, organizationId)
            ps.setObject(params++, study.id)
            ps.setObject(params++, Principals.getCurrentUser().id)
            ps.setString(params++, mapper.writeValueAsString(study.settings))
            ps.addBatch()
        }
        return ps.executeBatch().sum()
    }

    override fun getStudy(studyIds: Collection<UUID>): Iterable<Study> {
        return storageResolver
            .getStudyIdsByDataSourceName(studyIds)
            .flatMap { (dataSourceName, studyIdsForDataSource) ->
                val (flavor, hds) = storageResolver.getDataSource(dataSourceName)
                check(flavor == PostgresFlavor.VANILLA) { "Only vanilla postgres is supported for studies." }
                BasePostgresIterable(
                    PreparedStatementHolderSupplier(hds, GET_STUDIES_SQL, 256) { ps ->
                        val pgStudyIds = PostgresArrays.createUuidArray(ps.connection, studyIdsForDataSource)
                        ps.setArray(1, pgStudyIds)
                        ps.executeQuery()
                    }
                ) { ResultSetAdapters.study(it) }

            }
    }

    override fun updateStudy(connection: Connection, studyId: UUID, study: StudyUpdate) {
        val ps = connection.prepareStatement(UPDATE_STUDY_SQL)
        var index = 1;
        ps.setString(index++, study.title)
        ps.setString(index++, study.description)
        ps.setObject(index++, OffsetDateTime.now())
        ps.setObject(index++, study.startedAt)
        ps.setObject(index++, study.endedAt)
        ps.setObject(index++, study.lat)
        ps.setObject(index++, study.lon)
        ps.setString(index++, study.group)
        ps.setString(index++, study.version)
        ps.setString(index++, study.contact)
        if (study.settings == null){
            ps.setObject(index++, study.settings)
        }
        else {
            ps.setString(index++, mapper.writeValueAsString(study.settings))
        }
        ps.setObject(index++, studyId)
        ps.executeUpdate()
    }
}