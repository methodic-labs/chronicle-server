package com.openlattice.users

import com.auth0.json.mgmt.users.User
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicates
import com.openlattice.chronicle.authorization.*
import com.openlattice.chronicle.authorization.principals.PrincipalMapstore
import com.openlattice.chronicle.authorization.principals.SecurePrincipalsManager
import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.openlattice.chronicle.util.getPrincipal
import org.slf4j.LoggerFactory
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class Auth0SyncService(
        hazelcastInstance: HazelcastInstance,
        private val spm: SecurePrincipalsManager,
) {
    companion object {
        private val logger = LoggerFactory.getLogger(Auth0SyncService::class.java)
    }

    private val users = HazelcastMap.USERS.getMap(hazelcastInstance)
    private val principals = HazelcastMap.PRINCIPALS.getMap(hazelcastInstance)
    private val authnPrincipalCache = HazelcastMap.SECURABLE_PRINCIPALS.getMap(hazelcastInstance)
    private val authnRolesCache = HazelcastMap.RESOLVED_PRINCIPAL_TREES.getMap(hazelcastInstance)
    private val principalTrees = HazelcastMap.PRINCIPAL_TREES.getMap(hazelcastInstance)

    /**
     * Returns true, if the user initialization task has ran at and
     * [com.openlattice.authorization.mapstores.SecurablePrincipalsMapLoader] is populated.
     */
    fun usersInitialized(): Boolean {
        return authnPrincipalCache.isNotEmpty()
    }

    fun getCachedUsers(): Sequence<User> {
        return users.values.asSequence()
    }

    fun syncUser(user: User) {
        updateUser(user)
        syncUserEnrollmentsAndAuthentication(user)
    }

    fun syncUsers(allUsers: Sequence<User>) {
        updateUsers(allUsers)
        syncUserEnrollmentsAndAuthentications(allUsers)
    }

    fun createOrUpdateUsers(usersToUpdate: Collection<User>) {

        val usersByPrincipal = usersToUpdate.associateBy { getPrincipal(it) }.toMutableMap()

        val existingUserPrincipals = spm.getSecurablePrincipals(usersByPrincipal.keys).map { it.principal }
        val principalsToCreate = usersByPrincipal.keys - existingUserPrincipals

        logger.debug("Creating users {} and updating principals {}",
                     principalsToCreate.map { it.id },
                     existingUserPrincipals.map { it.id }
        )

        val newlyCreatedUsers = principalsToCreate.filter {
            val wasSuccessfullyCreated = tryCreateNewUserPrincipal(usersByPrincipal.getValue(it), it)

            if (!wasSuccessfullyCreated) {
                usersByPrincipal.remove(it)
            }

            wasSuccessfullyCreated
        }.associateWith { usersByPrincipal.getValue(it) }

        users.putAll(usersToUpdate.associateBy { it.id })

    }

    private fun updateUsers(allUsers: Sequence<User>) {
        logger.info("Updating users in bulk")
        allUsers.chunked(1_000).forEach {
            val chunk = it.map { user ->
                ensureSecurablePrincipalExists(user)
                //Update the user in the users table
                user.id to user
            }.toMap()
            users.putAll( chunk )
        }
    }

    private fun updateUser(user: User) {
        logger.info("Updating user ${user.id}")
        ensureSecurablePrincipalExists(user)

        //Update the user in the users table
        users.set(user.id, user)
    }

    private fun syncUserEnrollmentsAndAuthentications(allUsers: Sequence<User>) {
        //Figure out which users need to be added to which organizations.
        //Since we don't want to do O( # organizations ) for each user, we need to lookup organizations on a per user
        //basis and see if the user needs to be added.
        val allUsersByPrincipal = allUsers.associateBy { user ->
            getPrincipal(user)
        }.filter {
            it.value.appMetadata != null
        }

        logger.info("Synchronizing enrollments and authentication cache for all users")


        allUsersByPrincipal.forEach { (principal, user) ->
            syncAuthenticationCache(principal.id)
        }
    }

    private fun syncUserEnrollmentsAndAuthentication(user: User) {
        //Figure out which users need to be added to which organizations.
        //Since we don't want to do O( # organizations ) for each user, we need to lookup organizations on a per user
        //basis and see if the user needs to be added.
        logger.info("Synchronizing enrollments and authentication cache for user ${user.id}")
        val principal = getPrincipal(user)

        syncAuthenticationCache(principal.id)
    }

    fun syncAuthenticationCacheForPrincipalIds(principalIds: Set<String>) {
        val principalsById = principalIds.associateWith { Principal(PrincipalType.USER, it) }
        val securablePrincipals = principals.entrySet(
                Predicates.`in`(
                        PrincipalMapstore.PRINCIPAL_INDEX,
                        *principalsById.values.toTypedArray()
                )
        ).associate { it.value.principal.id to it.value }
        authnPrincipalCache.putAll(securablePrincipals)
        authnRolesCache.putAll(getPrincipalTreesByPrincipalId(securablePrincipals.values.toSet()))
    }

    private fun syncAuthenticationCache(principalId: String) {
        val sp = principals.values(
                Predicates.equal(
                        PrincipalMapstore.PRINCIPAL_INDEX,
                        Principal(PrincipalType.USER, principalId)
                )
        ).firstOrNull() ?: return
        authnPrincipalCache.set(principalId, sp)
        val securablePrincipals = getAllPrincipals(sp)

        val currentPrincipals: NavigableSet<Principal> = TreeSet()
        currentPrincipals.add(sp.principal)
        securablePrincipals
                .asSequence()
                .map { it.principal }
                .forEach { currentPrincipals.add(it) }

        authnRolesCache.set(principalId, SortedPrincipalSet(currentPrincipals))
    }

    private fun getLayer(aclKeys: Set<AclKey>): AclKeySet {
        return AclKeySet(principalTrees.getAll(aclKeys).values.flatten().toMutableSet())
    }

    private fun getPrincipalTreesByPrincipalId(sps: Set<SecurablePrincipal>): Map<String, SortedPrincipalSet> {
        val aclKeyPrincipals = mutableMapOf<AclKey,AclKeySet>()

        // Bulk load all relevant principal trees from hazelcast
        var nextLayer = sps.mapTo(mutableSetOf()) { it.aclKey }
        while (nextLayer.isNotEmpty()) {
            //Don't load what's already been loaded.
            val nextLayerMap = principalTrees.getAll(nextLayer - aclKeyPrincipals.keys)
            nextLayer = nextLayerMap.values.flatten().toMutableSet()
            aclKeyPrincipals.putAll(nextLayerMap)
        }

        // Map all loaded principals to SecurablePrincipals
        val aclKeysToPrincipals = principals.getAll(aclKeyPrincipals.keys + aclKeyPrincipals.values.flatten())

        // Map each SecurablePrincipal to all its aclKey children from the in-memory map, and from there a SortedPrincipalSet
        return sps.associate { sp ->
            val childAclKeys = mutableSetOf(sp.aclKey) //Need to include self.
            aclKeyPrincipals.getOrDefault(sp.aclKey, AclKeySet()).forEach { childAclKeys.add(it) }

            var nextAclKeyLayer : Set<AclKey> = childAclKeys

            while (nextAclKeyLayer.isNotEmpty()) {
                nextAclKeyLayer = (nextAclKeyLayer.flatMapTo(mutableSetOf()) {
                    aclKeyPrincipals[it] ?: setOf()
                }) - childAclKeys
                childAclKeys += nextAclKeyLayer
            }

            val sortedPrincipals = SortedPrincipalSet(TreeSet(childAclKeys.mapNotNull { aclKey ->
                aclKeysToPrincipals[aclKey]?.principal
            }))

            if (childAclKeys.size != sortedPrincipals.size) {
                logger.warn("Unable to retrieve principals for acl keys: ${childAclKeys - aclKeysToPrincipals.keys}")
            }

            sp.principal.id to sortedPrincipals
        }
    }

    private fun getAllPrincipals(sp: SecurablePrincipal): Collection<SecurablePrincipal> {
        val roles = getLayer(setOf(sp.aclKey))
        var nextLayer: Set<AclKey> = roles

        while (nextLayer.isNotEmpty()) {
            nextLayer = getLayer(nextLayer) - roles
            roles.addAll(nextLayer)
        }

        return principals.getAll(roles).values
    }

    private fun tryCreateNewUserPrincipal(user: User, principal: Principal): Boolean {
        try {
            val title = if (!user.nickname.isNullOrEmpty()) {
                user.nickname
            } else {
                user.email
            }

            spm.createSecurablePrincipalIfNotExists(
                    principal,
                    SecurablePrincipal(Optional.empty(), principal, title, Optional.empty())
            )

        } catch (e: Exception) {
            logger.error("Unable to create user {} with principal {}", user, principal, e)
            return false
        }

        return true
    }

    private fun ensureSecurablePrincipalExists(user: User): Principal {
        val principal = getPrincipal(user)

        if (!spm.principalExists(principal)) {
            tryCreateNewUserPrincipal(user, principal)
        }

        return principal
    }

}
