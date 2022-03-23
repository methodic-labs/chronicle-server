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
package com.openlattice.chronicle.authorization

import com.google.common.base.Stopwatch
import com.google.common.collect.ImmutableSet
import com.google.common.collect.Sets
import com.google.common.eventbus.EventBus
import com.openlattice.chronicle.ChronicleServerTests
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.authorization.principals.HazelcastPrincipalService
import com.openlattice.chronicle.authorization.principals.HazelcastPrincipalsMapManager
import com.openlattice.chronicle.authorization.principals.PrincipalsMapManager
import com.openlattice.chronicle.authorization.principals.SecurePrincipalsManager
import com.openlattice.chronicle.authorization.reservations.AclKeyReservationService
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.util.TestDataFactory
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import java.util.function.Function
import java.util.stream.Collectors
import kotlin.streams.asSequence

open class HzAuthzTest : ChronicleServerTests() {
    @Test
    fun testAddEntitySetPermission() {
        val key = AclKey(UUID.randomUUID())
        val permissions: EnumSet<Permission> = EnumSet.of(Permission.MATERIALIZE, Permission.READ)
        Assert.assertFalse(
            hzAuthz!!.checkIfHasPermissions(key, ImmutableSet.of(p), permissions)
        )
        hzAuthz!!.addPermission(key, p, permissions)
        Assert.assertTrue(
            hzAuthz!!.checkIfHasPermissions(key, ImmutableSet.of(p), permissions)
        )
    }

    @Test
    fun testAddEntitySetsPermissions() {
        val aclKeys: Set<AclKey> = ImmutableSet.of(
            AclKey(UUID.randomUUID()),
            AclKey(UUID.randomUUID()),
            AclKey(UUID.randomUUID())
        )
        val permissions: EnumSet<Permission> = EnumSet.of<Permission>(Permission.MATERIALIZE, Permission.READ)
        aclKeys.forEach(Consumer { key: AclKey? ->
            Assert.assertFalse(
                hzAuthz!!.checkIfHasPermissions(key!!, ImmutableSet.of(p), permissions)
            )
        })
        hzAuthz!!.addPermissions(aclKeys, p, permissions, SecurableObjectType.Study)
        aclKeys.forEach(Consumer { key: AclKey? ->
            Assert.assertTrue(
                hzAuthz!!.checkIfHasPermissions(key!!, ImmutableSet.of(p), permissions)
            )
        })
    }

    @Test
    fun testTypeMismatchPermission() {
        val key = AclKey(UUID.randomUUID())
        val permissions: EnumSet<Permission> = EnumSet.of(Permission.MATERIALIZE, Permission.READ)
        Assert.assertFalse(hzAuthz.checkIfHasPermissions(key, ImmutableSet.of(p), permissions))

        hzAuthz.createUnnamedSecurableObject(key, p, EnumSet.noneOf(Permission::class.java), SecurableObjectType.Study)
        hzAuthz.addPermission(key, p, permissions)
        val badkey: UUID = UUID.randomUUID()
        Assert.assertFalse(hzAuthz.checkIfHasPermissions(AclKey(badkey), ImmutableSet.of(p), permissions))
    }

    @Test
    fun testRemovePermissions() {
        val key = AclKey(UUID.randomUUID())
        val permissions: EnumSet<Permission> =
            EnumSet.of(Permission.MATERIALIZE, Permission.READ, Permission.OWNER)
        Assert.assertFalse(
            hzAuthz!!.checkIfHasPermissions(key, ImmutableSet.of(p), permissions)
        )
        Assert.assertFalse(
            hzAuthz!!.checkIfHasPermissions(key, ImmutableSet.of(p2), permissions)
        )

        hzAuthz.createUnnamedSecurableObject(key, p, EnumSet.noneOf(Permission::class.java), SecurableObjectType.Study)

        hzAuthz!!.addPermission(key, p, permissions)
        hzAuthz!!.addPermission(key, p2, permissions)
        Assert.assertTrue(
            hzAuthz!!.checkIfHasPermissions(AclKey(key), ImmutableSet.of(p), permissions)
        )
        Assert.assertTrue(
            hzAuthz!!.checkIfHasPermissions(AclKey(key), ImmutableSet.of(p2), permissions)
        )
        hzAuthz!!.removePermission(key, p, permissions)
        Assert.assertFalse(
            hzAuthz!!.checkIfHasPermissions(key, ImmutableSet.of(p), permissions)
        )
    }

    @Test
    fun testSetPermissions() {
        val key: AclKey = AclKey(UUID.randomUUID())
        val permissions: EnumSet<Permission> =
            EnumSet.of<Permission>(Permission.MATERIALIZE, Permission.READ, Permission.OWNER)
        val badPermissions: EnumSet<Permission> =
            EnumSet.of<Permission>(Permission.MATERIALIZE, Permission.READ, Permission.LINK)
        Assert.assertFalse(
            hzAuthz!!.checkIfHasPermissions(key, ImmutableSet.of(p), permissions)
        )
        Assert.assertFalse(
            hzAuthz!!.checkIfHasPermissions(key, ImmutableSet.of(p2), permissions)
        )
        hzAuthz.createUnnamedSecurableObject(key, p, EnumSet.noneOf(Permission::class.java), SecurableObjectType.Study)
        hzAuthz!!.setPermission(key, p, permissions)
        hzAuthz!!.setPermission(key, p2, permissions)
        Assert.assertFalse(
            hzAuthz!!.checkIfHasPermissions(key, ImmutableSet.of(p), badPermissions)
        )
        Assert.assertTrue(
            hzAuthz!!.checkIfHasPermissions(key, ImmutableSet.of(p), permissions)
        )
        Assert.assertFalse(
            hzAuthz!!.checkIfHasPermissions(key, ImmutableSet.of(p2), badPermissions)
        )
        Assert.assertTrue(
            hzAuthz!!.checkIfHasPermissions(key, ImmutableSet.of(p2), permissions)
        )
    }

    @Test
    open fun testListSecurableObjects() {
        val key: AclKey = AclKey(UUID.randomUUID())
        val p1 = initializePrincipal(TestDataFactory.userPrincipal())
        val p2 = initializePrincipal(TestDataFactory.userPrincipal())
        val permissions1: EnumSet<Permission> = EnumSet.of<Permission>(Permission.MATERIALIZE, Permission.READ)
        val permissions2: EnumSet<Permission> = EnumSet
            .of<Permission>(Permission.MATERIALIZE, Permission.READ, Permission.WRITE, Permission.OWNER)
        Assert.assertFalse(
            hzAuthz!!.checkIfHasPermissions(key, ImmutableSet.of(p1), permissions1)
        )
        Assert.assertFalse(
            hzAuthz!!.checkIfHasPermissions(key, ImmutableSet.of(p2), permissions2)
        )
        hzAuthz.createUnnamedSecurableObject(key, p1, EnumSet.noneOf(Permission::class.java), SecurableObjectType.Study)
        hzAuthz!!.addPermission(key, p1, permissions1)
        hzAuthz!!.addPermission(key, p2, permissions2)
        Assert.assertTrue(
            hzAuthz!!.checkIfHasPermissions(key, ImmutableSet.of(p1), permissions1)
        )
        Assert.assertFalse(
            hzAuthz!!.checkIfHasPermissions(
                key,
                ImmutableSet.of(p1),
                EnumSet.of(Permission.WRITE, Permission.OWNER)
            )
        )
        Assert.assertTrue(
            hzAuthz!!.checkIfHasPermissions(key, ImmutableSet.of(p2), permissions2)
        )
        val p1Owned = hzAuthz!!.getAuthorizedObjectsOfType(
            ImmutableSet.of(p1),
            SecurableObjectType.Study,
            EnumSet.of(Permission.OWNER)
        )
        val p1s: Set<List<UUID>> = p1Owned.collect(Collectors.toSet<List<UUID>>())
        if (p1s.isNotEmpty()) {
            val permissions = hzAuthz!!.getSecurableObjectPermissions(key, ImmutableSet.of(p1))
            Assert.assertTrue(permissions.contains(Permission.OWNER))
            Assert.assertTrue(
                hzAuthz!!.checkIfHasPermissions(key, ImmutableSet.of(p1), EnumSet.of<Permission>(Permission.OWNER))
            )
        }
        val p2Owned = hzAuthz!!.getAuthorizedObjectsOfType(
            ImmutableSet.of(p2),
            SecurableObjectType.Study,
            EnumSet.of(Permission.OWNER)
        )
        val p2s: Set<List<UUID>> = p2Owned.collect(Collectors.toSet<List<UUID>>())
        Assert.assertTrue(p1s.isEmpty())
        Assert.assertEquals(1, p2s.size.toLong())
        Assert.assertFalse(p1s.contains(key))
        Assert.assertTrue(p2s.contains(key))
    }

    @Test
    fun testAccessChecks() {
        val size = 100
        val accessChecks: MutableSet<AccessCheck> = HashSet(size)
        val aclKeys = arrayOfNulls<AclKey>(size)
        //        Principal[] p1s = new Principal[ size ];
        val p2s = arrayOfNulls<Principal>(size)
        val permissions1s: Array<EnumSet<Permission>?> = arrayOfNulls(size)
        val permissions2s: Array<EnumSet<Permission>?> = arrayOfNulls(size)
        val all: EnumSet<Permission> = EnumSet.noneOf<Permission>(
            Permission::class.java
        )
        for (i in 0 until size) {
            val key: AclKey = AclKey(UUID.randomUUID())
            val user1 = initializePrincipal(TestDataFactory.userPrincipal())
            val user2 = initializePrincipal(TestDataFactory.userPrincipal())
            aclKeys[i] = key
            //            p1s[ i ] = p1;
            p2s[i] = user2
            permissions1s[i] = TestDataFactory.nonEmptyPermissions()
            val permissions1: EnumSet<Permission> = permissions1s[i]!!
            permissions2s[i] = TestDataFactory.nonEmptyPermissions()
            val permissions2: EnumSet<Permission> = permissions2s[i]!!
            all.addAll(permissions2)
            Assert.assertFalse(
                hzAuthz!!.checkIfHasPermissions(key, ImmutableSet.of(user1), permissions1)
            )
            Assert.assertFalse(
                hzAuthz!!.checkIfHasPermissions(key, ImmutableSet.of(user2), permissions2)
            )
            hzAuthz.createUnnamedSecurableObject(
                key,
                user1,
                EnumSet.noneOf(Permission::class.java),
                SecurableObjectType.Study
            )
            hzAuthz!!.addPermission(key, user1, permissions1)
            hzAuthz!!.addPermission(key, user2, permissions2)
            Assert.assertTrue(hzAuthz!!.checkIfHasPermissions(key, ImmutableSet.of(user1), permissions1))
            Assert.assertEquals(
                permissions1.containsAll(permissions2),
                hzAuthz!!.checkIfHasPermissions(key, ImmutableSet.of(user1), permissions2)
            )
            Assert.assertTrue(hzAuthz!!.checkIfHasPermissions(key, ImmutableSet.of(user2), permissions2))
            val ac = AccessCheck(key, permissions1)
            accessChecks.add(ac)
        }
        var i = 0
        val keys = Arrays.asList(*aclKeys)
        for (ac in accessChecks) {
            val key = ac.aclKey
            //            Principal p1 = p1s[ i ];
            i = keys.indexOf(key)
            val p2 = p2s[i]!!
            val permissions1: EnumSet<Permission> = permissions1s[i]!!
            val permissions2: EnumSet<Permission> = permissions2s[i++]!!
            val result = hzAuthz
                .accessChecksForPrincipals(ImmutableSet.of(ac), ImmutableSet.of(p2))
                .associate { it.aclKey to EnumMap(it.permissions) }
            Assert.assertTrue(result.containsKey(key))
            val checkForKey: EnumMap<Permission, Boolean> = result.getValue(key)
            Assert.assertTrue(checkForKey.size == ac.permissions.size)
            Assert.assertTrue(checkForKey.keys.containsAll(ac.permissions))
            val overlapping: Set<Permission> = ImmutableSet.copyOf(Sets.intersection(permissions2, ac.permissions))
            Assert.assertTrue(overlapping.stream().allMatch { p: Permission -> result.getValue(key).getValue(p) })
            //            Assert.assertTrue( result.get( key ).get( Permission.MATERIALIZE ) );
            //            Assert.assertTrue( result.get( key ).get( Permission.READ ) );
            //            Assert.assertFalse( result.get( key ).get( Permission.OWNER ) );
        }
        val result = hzAuthz
            .accessChecksForPrincipals(accessChecks, ImmutableSet.copyOf(p2s.filterNotNull()))
            .associate { it.aclKey to EnumMap(it.permissions) }

        val w = Stopwatch.createStarted()
        logger.info("Elapsed time to access check: {} ms", w.elapsed(TimeUnit.MILLISECONDS))
        Assert.assertTrue(result.keys.containsAll(aclKeys.filterNotNull()))
    }

    @Test
    fun testGetAuthorizedPrincipalsOnSecurableObject() {
        val key = AclKey(UUID.randomUUID())
        val user1 = initializePrincipal(TestDataFactory.userPrincipal())
        val user2 = initializePrincipal(TestDataFactory.userPrincipal())
        val permissions = EnumSet.of(Permission.READ)
        hzAuthz.addPermission(key, user1, permissions)
        hzAuthz.addPermission(key, user2, permissions)
        val authorizedPrincipals = PrincipalSet(
            hzAuthz.getAuthorizedPrincipalsOnSecurableObject(key, permissions).toMutableSet()
        )
        Assert.assertEquals(setOf(user1, user2), authorizedPrincipals)
    }

    @Test
    fun testGetSecurableObjectSetsPermissions() {
        val key1: AclKey = AclKey(UUID.randomUUID())
        val key2: AclKey = AclKey(UUID.randomUUID())
        val key3: AclKey = AclKey(UUID.randomUUID())
        val key4: AclKey = AclKey(UUID.randomUUID())
        val key5: AclKey = AclKey(UUID.randomUUID())
        val key6: AclKey = AclKey(UUID.randomUUID())
        val principal = initializePrincipal(TestDataFactory.userPrincipal())
        val read: EnumSet<Permission> = EnumSet.of<Permission>(Permission.READ)
        val write: EnumSet<Permission> = EnumSet.of<Permission>(Permission.WRITE)
        val owner: EnumSet<Permission> = EnumSet.of<Permission>(Permission.OWNER)
        val materialize: EnumSet<Permission> = EnumSet.of<Permission>(Permission.MATERIALIZE)
        val discover: EnumSet<Permission> = EnumSet.of<Permission>(Permission.MATERIALIZE)

        // has read for all 3 acls, owner for 2, write for 2
        val aclKeySet1 = java.util.Set.of(key1, key2, key3)
        hzAuthz!!.addPermission(key1, principal, read)
        hzAuthz!!.addPermission(key2, principal, read)
        hzAuthz!!.addPermission(key3, principal, read)
        hzAuthz!!.addPermission(key1, principal, write)
        hzAuthz!!.addPermission(key2, principal, write)
        hzAuthz!!.addPermission(key2, principal, owner)
        hzAuthz!!.addPermission(key3, principal, owner)

        // has all 3 on one, none on other
        val aclKeySet2 = java.util.Set.of(key4, key5)
        hzAuthz!!.addPermission(key4, principal, materialize)
        hzAuthz!!.addPermission(key4, principal, discover)

        // no permissions at all
        val aclKeySet3 = java.util.Set.of(key5, key6)
        val reducedPermissionsMap1: Map<Set<AclKey>, EnumSet<Permission>> = hzAuthz!!.getSecurableObjectSetsPermissions(
            java.util.List.of(aclKeySet1, aclKeySet2, aclKeySet3),
            java.util.Set.of(principal)
        )
        Assert.assertEquals(read, reducedPermissionsMap1[aclKeySet1])
        Assert.assertEquals(EnumSet.noneOf<Permission>(Permission::class.java), reducedPermissionsMap1[aclKeySet2])
        Assert.assertEquals(EnumSet.noneOf<Permission>(Permission::class.java), reducedPermissionsMap1[aclKeySet3])

        // different principals permissions should accumulate toghether
        val p1 = initializePrincipal(TestDataFactory.userPrincipal())
        val p2 = initializePrincipal(TestDataFactory.userPrincipal())
        val p3 = initializePrincipal(TestDataFactory.userPrincipal())
        hzAuthz!!.addPermission(key1, p1, read)
        hzAuthz!!.addPermission(key1, p1, write)
        hzAuthz!!.addPermission(key2, p2, read)
        hzAuthz!!.addPermission(key2, p2, owner)
        hzAuthz!!.addPermission(key3, p3, read)
        hzAuthz!!.addPermission(key3, p3, materialize)
        val reducedPermissionsMap2: Map<Set<AclKey>, EnumSet<Permission>> = hzAuthz!!.getSecurableObjectSetsPermissions(
            java.util.List.of(aclKeySet1),
            java.util.Set.of(p1, p2, p3)
        )
        Assert.assertEquals(read, reducedPermissionsMap2[aclKeySet1])
    }

    companion object {
        private val ORG_ID = UUID.randomUUID()
        private val logger = LoggerFactory.getLogger(HzAuthzTest::class.java)
        private val adminPrincipal = initializePrincipal(SystemRole.ADMIN.principal)
        private val p = initializePrincipal(
            Principal(
                PrincipalType.USER,
                "grid|TRON"
            )
        )
        private val p2 = initializePrincipal(
            Principal(
                PrincipalType.USER,
                "grid|TRON2"
            )
        )
        private var isInitialized = false

        @JvmStatic
        protected lateinit var hzAuthz: HazelcastAuthorizationService
        protected lateinit var spm: SecurePrincipalsManager

        @JvmStatic
        fun initializePrincipal(principal: Principal): Principal {
            init()
            val sp: SecurablePrincipal = if (principal.type == PrincipalType.ROLE) Role(
                AclKey(ORG_ID, UUID.randomUUID()),
                principal,
                principal.id,
                Optional.empty()
            ) else SecurablePrincipal(
                Optional.empty<UUID>(),
                principal,
                principal.id,
                Optional.empty<String>()
            )
            try {
                spm.createSecurablePrincipalIfNotExists(sp)
            } catch (e: Exception) {
                logger.debug("could not initialize principal {}", principal, e)
            }
            return principal
        }

        @JvmStatic
        @BeforeClass
        fun init() {
            if (isInitialized) {
                return
            }
            val reservationService = AclKeyReservationService(sr)
            val principalsMapManager: PrincipalsMapManager = HazelcastPrincipalsMapManager(
                hazelcastInstance,
                reservationService
            )

            hzAuthz = HazelcastAuthorizationService(
                hazelcastInstance,
                testServer.context.getBean(StorageResolver::class.java),
                testServer.context.getBean(EventBus::class.java),
                principalsMapManager
            )
            spm = HazelcastPrincipalService(
                hazelcastInstance,
                reservationService,
                hzAuthz,
                principalsMapManager,
                testServer.context.getBean(AuditingManager::class.java)
            )

            isInitialized = true
        }
    }
}