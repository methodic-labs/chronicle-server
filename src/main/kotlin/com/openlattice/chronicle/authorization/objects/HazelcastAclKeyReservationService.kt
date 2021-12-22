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
package com.openlattice.chronicle.authorization.objects

import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableSet
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.openlattice.chronicle.authorization.AbstractSecurableObject
import com.openlattice.chronicle.authorization.SecurableObjectType
import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.openlattice.chronicle.util.getSafely
import com.openlattice.controllers.exceptions.TypeExistsException
import com.openlattice.controllers.exceptions.UniqueIdConflictException
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.util.*
import java.util.function.Supplier

class HazelcastAclKeyReservationService(hazelcast: HazelcastInstance) {
    companion object {
        private const val PRIVATE_NAMESPACE = "_private"

        /**
         * This keeps mapping between SecurableObjectTypes that aren't associated to names and their placeholder names.
         */
        private val RESERVED_NAMES_AS_MAP: EnumMap<SecurableObjectType, String> = EnumMap<SecurableObjectType, String>(
                SecurableObjectType::class.java
        )
        private val RESERVED_NAMES: Set<String> = ImmutableSet
                .copyOf<String>(RESERVED_NAMES_AS_MAP.values)

        /*
     * List of name associated types.
     */
        private val NAME_ASSOCIATED_TYPES: EnumSet<SecurableObjectType> = EnumSet
                .of<SecurableObjectType>(SecurableObjectType.Principal)

        private fun getPlaceholder(objName: String): String {
            return PRIVATE_NAMESPACE + "." + objName
        }

        init {
            for (objectType in SecurableObjectType.values()) {
                if (!NAME_ASSOCIATED_TYPES.contains(objectType)) {
                    RESERVED_NAMES_AS_MAP[objectType] = getPlaceholder(objectType.name)
                }
            }
        }
    }

    private val aclKeys: IMap<String, UUID> = HazelcastMap.ACL_KEYS.getMap(hazelcast)
    private val names: IMap<UUID, String> = HazelcastMap.NAMES.getMap(hazelcast)

    fun getId(name: String): UUID? {
        return getSafely(aclKeys, name)
    }

    fun getIds(names: Set<String>): Collection<UUID> {
        return aclKeys.getAll(names).values
    }

    fun getIdsByFqn(names: Set<String>): Map<String, UUID> {
        return aclKeys.getAll(names)
    }

    fun isReserved(name: String): Boolean {
        return aclKeys.containsKey(name)
    }

    fun renameReservation(oldName: String, newName: String) {
        Preconditions.checkArgument(!RESERVED_NAMES.contains(newName), "Cannot rename to a reserved name")
        Preconditions.checkArgument(!RESERVED_NAMES.contains(oldName), "Cannot rename a reserved name")

        /*
         * Attempt to associated newName with existing aclKey
         */
        val associatedAclKey: UUID = checkNotNull(getSafely(aclKeys, oldName)) {
            "Name $oldName is not being used yet."
        }
        val existingAclKey: UUID? = aclKeys.putIfAbsent(newName, associatedAclKey)

        if (existingAclKey == null) {
            aclKeys.delete(oldName)
            names.put(associatedAclKey, newName)
        } else {
            throw TypeExistsException(
                    "Cannot rename " + oldName + " to existing type "
                            + newName
            )
        }
    }

    fun renameReservation(id: UUID, newFqn: FullQualifiedName) {
        renameReservation(id, newFqn.fullQualifiedNameAsString)
    }

    fun renameReservation(id: UUID, newName: String) {
        Preconditions.checkArgument(!RESERVED_NAMES.contains(newName), "Cannot rename to a reserved name")
        val oldName: String = checkNotNull(getSafely(names, id)) {
            "This aclKey does not correspond to any type."
        }

        val existingAclKey: UUID? = aclKeys.putIfAbsent(newName, id)
        if (existingAclKey == null) {
            aclKeys.delete(oldName)
            names.put(id, newName)
        } else {
            throw TypeExistsException(
                    "Cannot rename " + oldName + " to existing type "
                            + newName
            )
        }
    }

    /**
     * This function reserves an `AclKey` for a SecurableObject that has a name. It throws unchecked exceptions
     * [TypeExistsException] if the type already exists with the same name or [UniqueIdConflictException]
     * if a different AclKey is already associated with the type.
     */
    fun <T : AbstractSecurableObject?> reserveIdAndValidateType(type: T, namer: Supplier<String>) {
        /*
         * Template this call and make wrappers that directly insert into type maps making fqns redundant.
         */
        val proposedName = namer.get()
        val currentName: String? = names.putIfAbsent(type!!.id, proposedName)
        if (currentName == null || proposedName == currentName) {
            /*
             * AclKey <-> Type association exists and is correct. Safe to try and register AclKey for type.
             */
            val existingAclKey: UUID? = aclKeys.putIfAbsent(proposedName, type.id)

            /*
             * Even if aclKey matches, letting two threads go through type creation creates potential problems when
             * entity types and entity sets are created using property types that have not quiesced. Easier for now to
             * just let one thread win and simplifies code path a lot.
             */if (existingAclKey != null && existingAclKey != type.id) {
                if (currentName == null) {
                    // We need to remove UUID reservation
                    names.delete(type.id)
                }
                throw TypeExistsException("Type $proposedName already exists.")
            }

            /*
             * AclKey <-> Type association exists and is correct. Type <-> AclKey association exists and is correct.
             * Only a single thread should ever reach here.
             */
        } else {
            throw UniqueIdConflictException("AclKey is already associated with different type.")
        }
    }

    /**
     * This function reserves an id for a SecurableObject. It throws unchecked exceptions
     * [TypeExistsException] if the type already exists or [UniqueIdConflictException] if a different AclKey
     * is already associated with the type.
     */
    fun reserveId(type: AbstractSecurableObject) {
        Preconditions.checkArgument(
                RESERVED_NAMES_AS_MAP.containsKey(type.category),
                "Unsupported securable type for reservation"
        )
        /*
         * Template this call and make wrappers that directly insert into type maps making fqns redundant.
         */
        val name: String? = names.putIfAbsent(type.id, RESERVED_NAMES_AS_MAP.getValue(type.category))

        /*
         * We don't care if FQN matches in this case as it provides us no additional validation information.
         */if (name != null) {
            throw UniqueIdConflictException("AclKey is already associated with different name.")
        }
    }

    /**
     * Releases a reserved id.
     *
     * @param id The id to release.
     */
    fun release(id: UUID) {
        val name: String? = names.remove(id)

        /*
         * We always issue the delete, even if sometimes there is no aclKey registered for that FQN.
         */if (name != null) {
            aclKeys.delete(name)
        }
    }
}