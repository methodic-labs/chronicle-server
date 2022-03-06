package com.openlattice.chronicle.authorization

import com.codahale.metrics.annotation.Timed
import com.google.common.eventbus.EventBus
import com.hazelcast.aggregation.Aggregators
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.openlattice.chronicle.authorization.aggregators.AuthorizationSetAggregator
import com.openlattice.chronicle.authorization.aggregators.PrincipalAggregator
import com.openlattice.chronicle.authorization.principals.PrincipalsMapManager
import com.openlattice.chronicle.hazelcast.HazelcastMap
import com.openlattice.chronicle.mapstores.authorization.PermissionMapstore.Companion.ACL_KEY_INDEX
import com.openlattice.chronicle.mapstores.authorization.PermissionMapstore.Companion.PERMISSIONS_INDEX
import com.openlattice.chronicle.mapstores.authorization.PermissionMapstore.Companion.PRINCIPAL_INDEX
import com.openlattice.chronicle.mapstores.authorization.PermissionMapstore.Companion.PRINCIPAL_TYPE_INDEX
import com.openlattice.chronicle.mapstores.authorization.PermissionMapstore.Companion.SECURABLE_OBJECT_TYPE_INDEX
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.PERMISSIONS
import com.openlattice.chronicle.storage.ChroniclePostgresTables.Companion.SECURABLE_OBJECTS
import com.openlattice.chronicle.storage.PostgresColumns
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.util.toAceKeys
import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.google.common.collect.*
import com.google.common.util.concurrent.MoreExecutors
import com.openlattice.chronicle.authorization.processors.*
import com.openlattice.chronicle.postgres.ResultSetAdapters
import com.openlattice.chronicle.storage.PostgresColumns.Companion.ACL_KEY
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PRINCIPAL_ID
import com.openlattice.chronicle.storage.PostgresColumns.Companion.PRINCIPAL_TYPE
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.sql.Connection
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.Executors
import java.util.function.Function
import java.util.stream.Collectors
import java.util.stream.Stream

@Service
class HazelcastAuthorizationService(
    hazelcastInstance: HazelcastInstance,
    storageResolver: StorageResolver,
    private val eventBus: EventBus,
    private val principalsMapManager: PrincipalsMapManager
) : AuthorizationManager {
    private val authorizationStorage = storageResolver.getDefaultPlatformStorage()
    private val aces: IMap<AceKey, AceValue> = HazelcastMap.PERMISSIONS.getMap(hazelcastInstance)
    private val securableObjectTypes = HazelcastMap.SECURABLE_OBJECT_TYPES.getMap(hazelcastInstance)

    companion object {
        private val logger = LoggerFactory.getLogger(HazelcastAuthorizationService::class.java)

        private val SECURABLE_OBJECT_COLS = listOf(
            ACL_KEY,
            PostgresColumns.SECURABLE_OBJECT_TYPE,
            PostgresColumns.SECURABLE_OBJECT_ID,
            PostgresColumns.SECURABLE_OBJECT_NAME
        ).joinToString(",") { it.name }

        /**
         *  1. acl key
         *  2. securable object type
         *  3. securable object id
         *  4. securable object name
         */
        private val INSERT_SECURABLE_OBJECT_SQL = """
            INSERT INTO ${SECURABLE_OBJECTS.name} VALUES (?,?,?,?) ON CONFLICT DO NOTHING
        """.trimIndent()

        /**
         *  1. acl key
         *  2. principal type
         *  3. principal id
         *  4. permissions
         *  5. expiration date
         */
        private val INSERT_ACES = """
            INSERT INTO ${PERMISSIONS.name} VALUES (?,?,?,?,?)
        """.trimIndent()

        private val DELETE_PRINCIPAL_PERMISSIONS = """
            DELETE FROM ${PERMISSIONS.name} WHERE ${PRINCIPAL_TYPE.name} = ? AND ${PRINCIPAL_ID.name} = ? 
            RETURNING ${ACL_KEY.name}
        """.trimIndent()

        private fun noAccess(permissions: EnumSet<Permission>): EnumMap<Permission, Boolean> {
            val pm = EnumMap<Permission, Boolean>(Permission::class.java)
            permissions.forEach { pm[it] = false }
            return pm
        }

        private fun matches(aclKeys: Collection<AclKey>, principals: Set<Principal>): Predicate<AceKey, AceValue> {
            return Predicates.and<AceKey, AceValue>(hasAnyAclKeys(aclKeys), hasAnyPrincipals(principals))
        }

        private fun matches(aclKey: AclKey, permissions: EnumSet<Permission>): Predicate<AceKey, AceValue> {
            return Predicates.and<AceKey, AceValue>(hasAclKey(aclKey), hasExactPermissions(permissions))
        }

        private fun hasExactPermissions(permissions: EnumSet<Permission>): Predicate<AceKey, AceValue> {

            val subPredicates = permissions
                .map { Predicates.equal<AceKey, AceValue>(PERMISSIONS_INDEX, it) }
                .toTypedArray()

            return Predicates.and(*subPredicates)
        }

        private fun hasAnyPrincipals(principals: Collection<Principal>): Predicate<AceKey, AceValue> {
            return Predicates.`in`<AceKey, AceValue>(PRINCIPAL_INDEX, *principals.toTypedArray())
        }

        private fun hasAnyAclKeys(aclKeys: Collection<AclKey>): Predicate<AceKey, AceValue> {
            return Predicates.`in`<AceKey, AceValue>(ACL_KEY_INDEX, *aclKeys.map { it.index }.toTypedArray())
        }

        private fun hasAclKey(aclKey: AclKey): Predicate<AceKey, AceValue> {
            return Predicates.equal(ACL_KEY_INDEX, aclKey.index)
        }

        private fun hasType(objectType: SecurableObjectType): Predicate<AceKey, AceValue> {
            return Predicates.equal(SECURABLE_OBJECT_TYPE_INDEX, objectType)
        }

        private fun hasAnyType(objectTypes: Collection<SecurableObjectType>): Predicate<AceKey, AceValue> {
            return Predicates.`in`(SECURABLE_OBJECT_TYPE_INDEX, *objectTypes.toTypedArray())
        }

        private fun hasPrincipal(principal: Principal): Predicate<AceKey, AceValue> {
            return Predicates.equal(PRINCIPAL_INDEX, principal)
        }

        private fun hasPrincipalType(type: PrincipalType): Predicate<AceKey, AceValue> {
            return Predicates.equal(PRINCIPAL_TYPE_INDEX, type)
        }
    }

    override fun createUnnamedSecurableObject(
        aclKey: AclKey,
        principal: Principal,
        permissions: EnumSet<Permission>,
        objectType: SecurableObjectType,
        expirationDate: OffsetDateTime
    ) {
        authorizationStorage.second.connection.use { connection ->
            createUnnamedSecurableObject(
                connection,
                aclKey,
                principal,
                permissions,
                objectType,
                expirationDate
            )
        }
        ensureAceIsLoaded(aclKey, principal)
    }

    override fun ensureAceIsLoaded(aclKey: AclKey, principal: Principal) {
        aces.loadAll(setOf(AceKey(aclKey, principal)), true)
    }

    override fun createUnnamedSecurableObject(
        connection: Connection,
        aclKey: AclKey,
        principal: Principal,
        permissions: EnumSet<Permission>,
        objectType: SecurableObjectType,
        expirationDate: OffsetDateTime
    ) {
        ensurePrincipalsExist(setOf(principal))
        val aclKeyArray = PostgresArrays.createUuidArray(connection, aclKey)

        /**
         * Only create the securable object entry if it hasn't already been created. Currently named objects from
         * [AbstractSecurableObject] get an entry here on registration, before the ACL is initialized.
         *
         * This is fine as in those cases we simply skip inserting the securable object and add permissions.
         *
         */
        if (!securableObjectTypes.containsKey(aclKey)) {
            val insertSecObj = connection.prepareStatement(INSERT_SECURABLE_OBJECT_SQL)

            insertSecObj.setArray(1, aclKeyArray)
            insertSecObj.setString(2, objectType.name)
            insertSecObj.setObject(3, aclKey.last())
            insertSecObj.setString(4, aclKey.last().toString()) //Unnamed objects so just use id as name
            insertSecObj.executeUpdate()
        }

        val insertPermissions = connection.prepareStatement(INSERT_ACES)
        insertPermissions.setArray(1, aclKeyArray)
        insertPermissions.setString(2, principal.type.name)
        insertPermissions.setString(3, principal.id)
        insertPermissions.setArray(4, PostgresArrays.createTextArray(connection, permissions.map { it.name }))
        insertPermissions.setObject(5, expirationDate)
        insertPermissions.executeUpdate()
    }

    /** Add Permissions **/

    override fun addPermission(key: AclKey, principal: Principal, permissions: EnumSet<Permission>) {
        addPermission(key, principal, permissions, OffsetDateTime.MAX)
    }

    override fun addPermission(
        key: AclKey,
        principal: Principal,
        permissions: Set<Permission>,
        expirationDate: OffsetDateTime
    ) {
        //TODO: We should do something better than reading the securable object type.
        val securableObjectType = getDefaultObjectType(securableObjectTypes, key)

        addPermission(key, principal, permissions, securableObjectType, expirationDate)
    }

    override fun addPermission(
        key: AclKey,
        principal: Principal,
        permissions: Set<Permission>,
        securableObjectType: SecurableObjectType,
        expirationDate: OffsetDateTime
    ) {
        ensurePrincipalsExist(setOf(principal))
        aces.executeOnKey(AceKey(key, principal), PermissionMerger(permissions, securableObjectType, expirationDate))
    }

    override fun addPermissions(
        keys: Set<AclKey>,
        principal: Principal,
        permissions: EnumSet<Permission>,
        securableObjectType: SecurableObjectType
    ) {
        addPermissions(keys, principal, permissions, securableObjectType, OffsetDateTime.MAX)
    }

    override fun addPermissions(
        keys: Set<AclKey>,
        principal: Principal,
        permissions: EnumSet<Permission>,
        securableObjectType: SecurableObjectType,
        expirationDate: OffsetDateTime
    ) {
        ensurePrincipalsExist(setOf(principal))
        val aceKeys = toAceKeys(keys, principal)
        aces.executeOnKeys(aceKeys, PermissionMerger(permissions, securableObjectType, expirationDate))
    }

    override fun addPermissions(acls: List<Acl>) {
        ensureAclPrincipalsExist(acls)
        val updates = getAceValueToAceKeyMap(acls)
        updates.keySet().forEach {
            val aceKeys = updates[it]
            aces.executeOnKeys(aceKeys, PermissionMerger(it.permissions, it.securableObjectType, it.expirationDate))
        }
    }

    /** Remove Permissions **/

    override fun removePermissions(acls: List<Acl>) {
        acls.map {
            AclKey(it.aclKey) to it.aces
                .filter { ace -> ace.permissions.contains(Permission.OWNER) }
                .map { ace -> ace.principal }
                .toSet()
        }.filter { (_, owners) -> owners.isNotEmpty() }.forEach { (aclKey, owners) ->
            ensureAclKeysHaveOtherUserOwners(ImmutableSet.of(aclKey), owners)
        }

        val updates = getAceValueToAceKeyMap(acls)

        updates.keySet().forEach {
            aces.executeOnKeys(updates[it], PermissionRemover(it.permissions))
        }
    }

    override fun removePermission(
        key: AclKey,
        principal: Principal,
        permissions: EnumSet<Permission>
    ) {
        if (permissions.contains(Permission.OWNER)) {
            ensureAclKeysHaveOtherUserOwners(setOf(key), setOf(principal))
        }


        aces.executeOnKey(AceKey(key, principal), PermissionRemover(permissions))
    }

    override fun deletePermissions(aclKey: AclKey) {
        securableObjectTypes.delete(aclKey)
        aces.removeAll(hasAclKey(aclKey))
    }

    override fun deletePrincipalPermissions(principal: Principal) {
        aces.removeAll(hasPrincipal(principal))
    }

    /** Set Permissions **/

    override fun setPermissions(acls: List<Acl>) {
        ensureAclPrincipalsExist(acls)
        val types = getSecurableObjectTypeMapForAcls(acls)

        val updates = mutableMapOf<AceKey, AceValue>()

        acls.forEach {
            val aclKey = AclKey(it.aclKey)
            val securableObjectType = getDefaultObjectType(types, aclKey)

            it.aces.forEach { ace: Ace ->
                val principal = ace.principal
                val permissions = EnumSet.copyOf(ace.permissions)
                updates[AceKey(aclKey, principal)] = AceValue(permissions, securableObjectType, ace.expirationDate)
            }
        }

        aces.putAll(updates)
    }

    private fun getSecurableObjectTypeMapForAcls(acls: Collection<Acl>): Map<AclKey, SecurableObjectType> {
        return securableObjectTypes.getAll(acls.map { it.aclKey }.toSet())
    }

    override fun setPermission(
        key: AclKey,
        principal: Principal,
        permissions: EnumSet<Permission>
    ) {
        setPermission(key, principal, permissions, OffsetDateTime.MAX)
    }

    override fun setPermission(
        key: AclKey,
        principal: Principal,
        permissions: EnumSet<Permission>,
        expirationDate: OffsetDateTime
    ) {
        ensurePrincipalsExist(setOf(principal))
        if (!permissions.contains(Permission.OWNER)) {
            ensureAclKeysHaveOtherUserOwners(setOf(key), setOf(principal))
        }

        //This should be a rare call to overwrite all permissions, so it's okay to do a read before write.
        val securableObjectType = getDefaultObjectType(securableObjectTypes, key)
        aces[AceKey(key, principal)] = AceValue(permissions, securableObjectType, expirationDate)
    }

    override fun setPermission(aclKeys: Set<AclKey>, principals: Set<Principal>, permissions: EnumSet<Permission>) {
        //This should be a rare call to overwrite all permissions, so it's okay to do a read before write.
        ensurePrincipalsExist(principals)
        if (!permissions.contains(Permission.OWNER)) {
            ensureAclKeysHaveOtherUserOwners(aclKeys, principals)
        }

        val securableObjectTypesForAclKeys = securableObjectTypes.getAll(aclKeys)
        val newPermissions: MutableMap<AceKey, AceValue> = HashMap(aclKeys.size * principals.size)

        aclKeys.forEach {
            val objectType = getDefaultObjectType(securableObjectTypesForAclKeys, it)
            val aceValue = AceValue(permissions, objectType, OffsetDateTime.MAX)

            principals.forEach { principal ->
                newPermissions[AceKey(it, principal)] = aceValue
            }
        }

        aces.putAll(newPermissions)
    }

    override fun setPermissions(permissions: Map<AceKey, EnumSet<Permission>>) {
        ensurePrincipalsExist(permissions.keys.mapTo(mutableSetOf()) { it.principal })

        permissions.entries
            .filter { entry -> !entry.value.contains(Permission.OWNER) }
            .groupBy { e -> e.key.aclKey }
            .mapValues { it.value.map { entry -> entry.key.principal }.toSet() }
            .forEach { (aclKey, principals) -> ensureAclKeysHaveOtherUserOwners(setOf(aclKey), principals) }

        val securableObjectTypesForAclKeys = securableObjectTypes.getAll(permissions.keys.map { it.aclKey }.toSet())

        val newPermissions: MutableMap<AceKey, AceValue> = Maps.newHashMap()

        permissions.forEach { (aceKey: AceKey, acePermissions: EnumSet<Permission>) ->
            val aclKey = aceKey.aclKey
            val objectType = getDefaultObjectType(securableObjectTypesForAclKeys, aclKey)
            newPermissions[aceKey] = AceValue(acePermissions, objectType, OffsetDateTime.MAX)
        }

        aces.putAll(newPermissions)
    }

    /*** AUTH CHECKS ***/

    @Timed
    override fun authorize(
        requests: Map<AclKey, EnumSet<Permission>>,
        principals: Set<Principal>
    ): MutableMap<AclKey, EnumMap<Permission, Boolean>> {

        val permissionMap = requests.mapValues { noAccess(it.value) }.toMutableMap()

        val aceKeys = requests.keys
            .flatMap { aclKey -> principals.map { principal -> AceKey(aclKey, principal) } }
            .toSet()

        aces
            .executeOnKeys(aceKeys, AuthorizationEntryProcessor())
            .forEach { (aceKey, permissions) ->
                val aclKeyPermissions = permissionMap.getValue(aceKey.aclKey)
                permissions.forEach { permission ->
                    aclKeyPermissions.computeIfPresent(permission) { _, _ -> true }
                }
            }

        return permissionMap
    }

    @Timed
    override fun accessChecksForPrincipals(
        accessChecks: Set<AccessCheck>,
        principals: Set<Principal>
    ): List<Authorization> {
        val requests: MutableMap<AclKey, EnumSet<Permission>> = Maps.newLinkedHashMapWithExpectedSize(accessChecks.size)

        accessChecks.forEach {
            val p = requests.getOrDefault(it.aclKey, EnumSet.noneOf(Permission::class.java))
            p.addAll(it.permissions)
            requests[it.aclKey] = p
        }

        return authorize(requests, principals).map { Authorization(it.key, it.value) }
    }

    @Timed
    override fun checkIfHasPermissions(
        key: AclKey,
        principals: Set<Principal>,
        requiredPermissions: EnumSet<Permission>
    ): Boolean {
        val aceKeys = principals.map { AceKey(key, it) }.toSet()

        return aces.executeOnKeys(aceKeys, AuthorizationEntryProcessor())
            .values
            .flatMap { it as DelegatedPermissionEnumSet }
            .toSet()
            .containsAll(requiredPermissions)
    }

    @Timed
    override fun getSecurableObjectSetsPermissions(
        aclKeySets: Collection<Set<AclKey>>,
        principals: Set<Principal>
    ): Map<Set<AclKey>, EnumSet<Permission>> {
        return aclKeySets.parallelStream().collect(Collectors.toMap<Set<AclKey>, Set<AclKey>, EnumSet<Permission>>(
            Function.identity(),
            Function { getSecurableObjectSetPermissions(it, principals) }
        ))
    }

    @Timed
    override fun getSecurableObjectPermissions(
        key: AclKey,
        principals: Set<Principal>
    ): Set<Permission> {
        val objectPermissions = EnumSet.noneOf(Permission::class.java)
        val aceKeys = principals.map { AceKey(key, it) }.toSet()
        aces.getAll(aceKeys).values.mapNotNull { it.permissions }.forEach { objectPermissions.addAll(it) }

        return objectPermissions
    }

    @Timed
    override fun getAuthorizedObjectsOfType(
        principal: Principal,
        objectType: SecurableObjectType,
        permissions: EnumSet<Permission>
    ): Stream<AclKey> {
        return getAuthorizedObjectsOfType(setOf(principal), objectType, permissions)
    }

    @Timed
    @Deprecated(
        message = "Deprecated inefficient version using stream",
        replaceWith = ReplaceWith("listAuthorizedObjectsOfType")
    )
    override fun getAuthorizedObjectsOfType(
        principals: Set<Principal>,
        objectType: SecurableObjectType,
        permissions: EnumSet<Permission>
    ): Stream<AclKey> {
        val principalPredicate = if (principals.size == 1) hasPrincipal(principals.first()) else hasAnyPrincipals(
            principals
        )
        val p = Predicates.and<AceKey, AceValue>(
            principalPredicate,
            hasType(objectType),
            hasExactPermissions(permissions)
        )

        return aces.keySet(p)
            .stream()
            .map { it.aclKey }
            .distinct()
    }

    @Timed
    override fun listAuthorizedObjectsOfType(
        principals: Set<Principal>,
        objectType: SecurableObjectType,
        permissions: EnumSet<Permission>
    ): List<AclKey> {
        val principalPredicate = if (principals.size == 1) hasPrincipal(principals.first()) else hasAnyPrincipals(
            principals
        )
        val p = Predicates.and<AceKey, AceValue>(
            principalPredicate,
            hasType(objectType),
            hasExactPermissions(permissions)
        )

        return aces.keySet(p).map { it.aclKey }
    }

    @Timed
    override fun getAuthorizedObjectsOfTypes(
        principals: Set<Principal>,
        objectTypes: Collection<SecurableObjectType>,
        permissions: EnumSet<Permission>
    ): Stream<AclKey> {
        val principalPredicate = if (principals.size == 1) hasPrincipal(principals.first()) else hasAnyPrincipals(
            principals
        )
        val p = Predicates.and<AceKey, AceValue>(
            principalPredicate,
            hasAnyType(objectTypes),
            hasExactPermissions(permissions)
        )

        return aces.keySet(p)
            .stream()
            .map { it.aclKey }
            .distinct()
    }

    @Timed
    override fun getAuthorizedObjectsOfType(
        principals: Set<Principal>,
        objectType: SecurableObjectType,
        permissions: EnumSet<Permission>,
        additionalFilter: Predicate<*, *>
    ): Stream<AclKey> {
        val p = Predicates.and<AceKey, AceValue>(
            hasAnyPrincipals(principals),
            hasType(objectType),
            hasExactPermissions(permissions),
            additionalFilter
        )
        return aces.keySet(p)
            .stream()
            .map { obj: AceKey -> obj.aclKey }
            .distinct()
    }

    @Timed
    override fun getAllSecurableObjectPermissions(key: AclKey): Acl {
        val acesWithPermissions = aces.entrySet(hasAclKey(key))
            .filter { it.value.isNotEmpty() }
            .map { Ace(it.key.principal, it.value.permissions) }
            .toSet()

        return Acl(key, acesWithPermissions)
    }

    @Timed
    override fun getAllSecurableObjectPermissions(keys: Set<AclKey>): Set<Acl> {
        return aces.entrySet(hasAnyAclKeys(keys))
            .filter { it.value.isNotEmpty() }
            .groupBy { it.key.aclKey }
            .mapTo(mutableSetOf()) { entry ->
                Acl(entry.key, entry.value.mapTo(mutableSetOf()) { Ace(it.key.principal, it.value.permissions) })
            }
    }

    @Timed
    override fun getAuthorizedPrincipalsOnSecurableObject(
        key: AclKey, permissions: EnumSet<Permission>
    ): Set<Principal> {
        val principalMap = mutableMapOf(key to PrincipalSet(mutableSetOf()))

        return aces.aggregate(PrincipalAggregator(principalMap), matches(key, permissions))
            .getResult()
            .getValue(key)
    }

    @Timed
    override fun getSecurableObjectOwners(key: AclKey): Set<Principal> {
        return getAuthorizedPrincipalsOnSecurableObject(key, EnumSet.of(Permission.OWNER))
    }

    @Timed
    override fun getOwnersForSecurableObjects(aclKeys: Collection<AclKey>): SetMultimap<AclKey, Principal> {
        val result: SetMultimap<AclKey, Principal> = HashMultimap.create()

        aces.keySet(Predicates.and(hasAnyAclKeys(aclKeys), hasExactPermissions(EnumSet.of(Permission.OWNER))))
            .forEach { result.put(it.aclKey, it.principal) }

        return result
    }

    override fun deleteAllPrincipalPermissions(principal: Principal) {
        /*
        This will delete from db and then evict from memory.
        Since we check if principal exists before adding a permission it should fail cleanly as long as principal
        was deleted before permissions were deleted.
        */
        BasePostgresIterable(
            PreparedStatementHolderSupplier(
                authorizationStorage.second,
                DELETE_PRINCIPAL_PERMISSIONS
            ) {
                it.setString(1, principal.type.name)
                it.setString(2, principal.id)
            }) { AceKey(ResultSetAdapters.aclKey(it), principal) }
            .forEach(aces::evict)
    }


    /** Private Helpers **/

    private fun ensureAclKeysHaveOtherUserOwners(aclKeys: Set<AclKey>, principals: Set<Principal>) {
        val userPrincipals = principals.stream().filter { p: Principal -> p.type == PrincipalType.USER }
            .collect(Collectors.toSet())
        if (userPrincipals.size > 0) {

            val allOtherUserOwnersPredicate = Predicates.and<AceKey, AceValue>(
                hasAnyAclKeys(aclKeys),
                hasExactPermissions(EnumSet.of(Permission.OWNER)),
                Predicates.not<AceKey, AceValue>(hasAnyPrincipals(userPrincipals)),
                hasPrincipalType(PrincipalType.USER)
            )

            val allOtherUserOwnersCount: Long = aces.aggregate(
                Aggregators.count<Map.Entry<AceKey, AceValue>>(), allOtherUserOwnersPredicate
            )
            check(allOtherUserOwnersCount != 0L) {
                "Unable to remove owner permissions as a securable object will be left without an owner of " +
                        "type USER"
            }
        }
    }

    private fun getAceValueToAceKeyMap(acls: List<Acl>): SetMultimap<AceValue, AceKey> {
        val map: SetMultimap<AceValue, AceKey> = HashMultimap.create()
        val types = getSecurableObjectTypeMapForAcls(acls)
        acls.forEach { acl: Acl ->
            val aclKey = AclKey(acl.aclKey)

            acl.aces.forEach {
                map.put(
                    AceValue(EnumSet.copyOf(it.permissions), getDefaultObjectType(types, aclKey), it.expirationDate),
                    AceKey(aclKey, it.principal)
                )
            }
        }
        return map
    }

    private fun getSecurableObjectSetPermissions(
        aclKeySet: Set<AclKey>,
        principals: Set<Principal>
    ): EnumSet<Permission> {

        val authorizationsMap = aclKeySet
            .associateWith { EnumSet.noneOf(Permission::class.java) }
            .toMutableMap()

        return aces.aggregate(AuthorizationSetAggregator(authorizationsMap), matches(aclKeySet, principals))
    }

    private fun getDefaultObjectType(map: Map<AclKey, SecurableObjectType>, aclKey: AclKey): SecurableObjectType {
        val securableObjectType = map.getOrDefault(aclKey, SecurableObjectType.Unknown)

        if (securableObjectType == SecurableObjectType.Unknown) {
            logger.warn("Unrecognized object type for acl key {} key ", aclKey)
        }

        return securableObjectType
    }

    private fun ensureAclPrincipalsExist(acls: List<Acl>) {
        val principals = acls.flatMap { it.aces }.mapTo(mutableSetOf()) { it.principal }
        ensurePrincipalsExist(principals)
    }

    private fun ensurePrincipalsExist(principals: Set<Principal>) {
        val nonexistentPrincipals = principals - principalsMapManager.getAclKeyByPrincipal(principals).keys
        check(nonexistentPrincipals.isEmpty()) {
            logger.error("Could not update permissions because principals $nonexistentPrincipals do not exist.")
        }
    }
}
