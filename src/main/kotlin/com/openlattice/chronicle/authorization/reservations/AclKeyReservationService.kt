/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */
package com.openlattice.chronicle.authorization.reservations

import com.geekbeast.util.LinearBackoff
import com.geekbeast.util.attempt
import com.google.common.base.Preconditions.checkState
import com.openlattice.chronicle.authorization.AbstractSecurableObject
import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.SECURABLE_OBJECTS
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ACL_KEY
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SECURABLE_OBJECT_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SECURABLE_OBJECT_NAME
import com.openlattice.chronicle.storage.PostgresColumns.Companion.SECURABLE_OBJECT_TYPE
import com.geekbeast.controllers.exceptions.TypeExistsException
import com.geekbeast.controllers.exceptions.UniqueIdConflictException
import com.openlattice.jdbc.DataSourceManager
import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import org.slf4j.LoggerFactory
import java.rmi.AlreadyBoundException
import java.sql.PreparedStatement
import java.util.*

class AclKeyReservationService(private val dsm: DataSourceManager) {
    companion object {
        private val logger = LoggerFactory.getLogger(AclKeyReservationService::class.java)
        private val INSERT_COLS = listOf(ACL_KEY, SECURABLE_OBJECT_TYPE, SECURABLE_OBJECT_ID, SECURABLE_OBJECT_NAME)
                .joinToString(",") { it.name }

        private val INSERT_SQL = """
        INSERT INTO ${SECURABLE_OBJECTS.name} ($INSERT_COLS) VALUES (?,?,?,?) ON CONFLICT DO NOTHING 
            RETURNING ${SECURABLE_OBJECT_ID.name}
    """

        /***
         * Updates the name of a securable object by name.
         *
         * 1. new name
         * 2. current name
         */
        private val UPDATE_BY_NAME_SQL = """
        UPDATE ${SECURABLE_OBJECTS.name} SET ${SECURABLE_OBJECT_NAME.name} = ? WHERE ${SECURABLE_OBJECT_NAME.name} = ?
    """

        /***
         * Updates the name of a securable object by id.
         *
         * 1. new name
         * 2. id
         */
        private val UPDATE_BY_ID_SQL = """
        UPDATE ${SECURABLE_OBJECTS.name} SET ${SECURABLE_OBJECT_NAME.name} = ? WHERE ${SECURABLE_OBJECT_ID.name} = ?
    """

        /***
         * Deletes a securable object by name
         *
         * 1. name of object to delete
         */
        private val DELETE_BY_NAME_SQL = """
        DELETE FROM ${SECURABLE_OBJECTS.name} WHERE ${SECURABLE_OBJECT_NAME.name} = ANY(?)
    """

        /***
         * Deletes a securable object by id
         *
         * 1. id
         */
        private val DELETE_BY_ID_SQL = """
        DELETE FROM ${SECURABLE_OBJECTS.name} WHERE ${SECURABLE_OBJECT_ID.name} = ANY(?)
    """

        private val SELECT_SQL = """
        SELECT ${SECURABLE_OBJECT_ID.name},${SECURABLE_OBJECT_NAME.name} FROM ${SECURABLE_OBJECTS.name} 
            WHERE ${SECURABLE_OBJECT_NAME.name} = ?    
    """
        private val SELECT_MULTIPLE_SQL = """
        SELECT ${SECURABLE_OBJECT_ID.name},${SECURABLE_OBJECT_NAME.name} FROM ${SECURABLE_OBJECTS.name} 
            WHERE ${SECURABLE_OBJECT_NAME.name} = ANY(?)    
    """
        private val SELECT_COUNT_SQL = """
        SELECT count(*) FROM ${SECURABLE_OBJECTS.name} 
            WHERE ${SECURABLE_OBJECT_NAME.name} = ?    
    """
    }


    fun getId(name: String): UUID {
        dsm.getDefaultDataSource().connection.use { conn ->
            conn.prepareStatement(SELECT_SQL).use { ps ->
                ps.setString(1, name)
                val rs = ps.executeQuery()
                rs.use {
                    checkState(rs.next()) {
                        "No id found for $name"
                    }
                    return ResultSetAdapters.securableObjectId(rs)
                }
            }
        }
    }

    fun getIds(names: Set<String>): Map<String, UUID> {
        return BasePostgresIterable(
                PreparedStatementHolderSupplier(
                        hds = dsm.getDefaultDataSource(),
                        sql = SELECT_MULTIPLE_SQL,
                        statementTimeoutMillis = 0L,
                        bind = { ps: PreparedStatement ->
                            ps.setArray(1, PostgresArrays.createTextArray(ps.connection, names))
                        })
        ) { ResultSetAdapters.securableObjectName(it) to ResultSetAdapters.securableObjectId(it) }.toMap()
    }

    fun isReserved(name: String): Boolean {
        dsm.getDefaultDataSource().connection.use { conn ->
            conn.prepareStatement(SELECT_COUNT_SQL).use { ps ->
                ps.setString(1, name)
                val rs = ps.executeQuery()
                rs.use {
                    checkState(rs.next()) {
                        "No id found for $name"
                    }
                    return ResultSetAdapters.count(rs) > 0
                }
            }
        }
    }

    fun renameReservation(oldName: String, newName: String) {
        dsm.getDefaultDataSource().connection.use { conn ->
            conn.prepareStatement(UPDATE_BY_NAME_SQL).use { ps ->
                ps.setString(1, newName)
                ps.setString(2, oldName)
                if (ps.executeUpdate() == 0) {
                    logger.warn("No securable object with name $oldName was found to update to $newName")
                }
            }
        }

    }

    fun renameReservation(id: UUID, newName: String) {
        dsm.getDefaultDataSource().connection.use { conn ->
            conn.prepareStatement(UPDATE_BY_ID_SQL).use { ps ->
                ps.setString(1, newName)
                ps.setObject(2, id)
                if (ps.executeUpdate() == 0) {
                    logger.warn("No securable object with id $id was found to update to $newName")
                }
            }
        }
    }


    fun <T : AbstractSecurableObject> registerSecurableObject(
            obj: T, prefix: AclKey = AclKey(), name: String
    ): UUID {
        return registerSecurableObject(obj, prefix) { name }
    }

    /**
     * This function reserves an [AclKey] for an [AbstractSecurableObject] that has a unique name. It throws unchecked exceptions
     * [TypeExistsException] if the type already exists with the same name or [UniqueIdConflictException]
     * if a different [AclKey] is already associated with the type.
     */
    fun <T : AbstractSecurableObject> registerSecurableObject(
            obj: T,
            prefix: AclKey = AclKey(),
            nameExtractor: (T) -> String = { it.title }
    ): UUID {
        val proposedName = nameExtractor(obj)
        //Back off 50ms each attempt for a maximum of 10 attempts, waiting no more than a second.
        for ( i in 0 until 10 ) {
            dsm.getDefaultDataSource().connection.use { conn ->
                val aclKey = AclKey(prefix + obj.id)
                conn.prepareStatement(INSERT_SQL).use { ps ->
                    ps.setArray(1, PostgresArrays.createUuidArray(conn, aclKey))
                    ps.setObject(2, obj.category.name)
                    ps.setObject(3, obj.id)
                    ps.setObject(4, proposedName)
                    val rs = ps.executeQuery()
                    //We check if the id and proposed name have been inserted appropriately
                    if (rs.next()) {
                        obj.id = ResultSetAdapters.securableObjectId(rs)
                        return obj.id
                    } else {
                        //Retry with a diffferent id
                        obj.id = UUID.randomUUID()
                    }
                }
            }
        }
        throw IllegalStateException("Unable to assign an id for object ${obj.title} of type ${obj.category}")
    }

    /**
     * Releases a reserved id.
     *
     * @param id The id to release.
     */
    fun release(id: UUID) {
        releaseByIds(listOf(id))
    }

    /**
     * Releases a reserved id.
     *
     * @param id The id to release.
     */
    fun releaseByIds(ids: Collection<UUID>) {
        dsm.getDefaultDataSource().connection.use { conn ->
            conn.prepareStatement(DELETE_BY_ID_SQL).use { ps ->
                ps.setArray(1, PostgresArrays.createUuidArray(conn, ids))
                if (ps.executeUpdate() == 0) {
                    logger.warn("Some securable objects with ids $ids were not found to delete")
                }
            }
        }
    }

    /**
     * Releases a reserved id.
     *
     * @param id The id to release.
     */
    fun release(name: String) {
        releaseByNames(listOf(name))
    }

    /**
     * Releases reserved ids.
     *
     * @param ids A collection of ids to release.
     */
    fun releaseByNames(names: Collection<String>) {
        dsm.getDefaultDataSource().connection.use { conn ->
            conn.prepareStatement(DELETE_BY_NAME_SQL).use { ps ->
                ps.setArray(1, PostgresArrays.createTextArray(conn, names))
                if (ps.executeUpdate() == 0) {
                    logger.warn("Some securable objects with names $names were not found to delete")
                }
            }
        }
    }
}