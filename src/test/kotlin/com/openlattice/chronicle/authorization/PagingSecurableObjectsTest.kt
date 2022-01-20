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

import com.openlattice.chronicle.util.TestDataFactory
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import org.slf4j.LoggerFactory
import java.util.*
import java.util.stream.Collectors

open class PagingSecurableObjectsTest : HzAuthzTest() {
    @Test
    override fun testListSecurableObjects() {
        val result = hzAuthz.getAuthorizedObjectsOfType(
            currentPrincipals,
            SecurableObjectType.Study,
            EnumSet.of(Permission.READ)
        ).collect(Collectors.toSet())
        Assert.assertEquals(3, result.size.toLong())
    }

    @Test
    fun testNoResults() {
        val result = hzAuthz.getAuthorizedObjectsOfType(
            currentPrincipals,
            SecurableObjectType.Organization,
            EnumSet.of(Permission.READ)
        ).collect(Collectors.toSet())
        Assert.assertEquals(0, result.size.toLong())
    }

    companion object {
        private val logger = LoggerFactory.getLogger(PagingSecurableObjectsTest::class.java)

        // Entity Set acl Keys
        protected val key1 = AclKey(UUID.randomUUID())
        protected val key2 = AclKey(UUID.randomUUID())
        protected val key3 = AclKey(UUID.randomUUID())

        // User and roles
        protected val u1 = TestDataFactory.userPrincipal()
        protected val r1 = TestDataFactory.rolePrincipal()
        protected val r2 = TestDataFactory.rolePrincipal()
        protected val r3 = TestDataFactory.rolePrincipal()
        protected val currentPrincipals: NavigableSet<Principal> = TreeSet()

        @BeforeClass
        @JvmStatic
        fun init() {
            HzAuthzTest.init()
            initializePrincipal(u1)
            initializePrincipal(r1)
            initializePrincipal(r2)
            initializePrincipal(r3)
            currentPrincipals.add(u1)
            currentPrincipals.add(r1)
            currentPrincipals.add(r2)
            currentPrincipals.add(r3)
            hzAuthz.addPermission(key1, u1, EnumSet.allOf(Permission::class.java))
            hzAuthz.setSecurableObjectType(key1, SecurableObjectType.Study)
            hzAuthz.addPermission(key2, r1, EnumSet.of(Permission.READ, Permission.WRITE))
            hzAuthz.setSecurableObjectType(key2, SecurableObjectType.Study)
            hzAuthz.addPermission(key3, r2, EnumSet.of(Permission.READ))
            hzAuthz.setSecurableObjectType(key3, SecurableObjectType.Study)
        }
    }
}