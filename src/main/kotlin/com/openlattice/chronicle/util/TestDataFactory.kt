package com.openlattice.chronicle.util

import com.google.common.collect.ImmutableList
import com.openlattice.chronicle.authorization.*
import com.openlattice.chronicle.organizations.OrganizationPrincipal
import org.apache.commons.text.CharacterPredicates
import org.apache.commons.text.RandomStringGenerator
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class TestDataFactory {
    companion object {
        private val actions = Action.values()
        private val r = Random()
        private val permissions = Permission.values()
        private val securableObjectTypes = SecurableObjectType.values()
        private val allowedDigitsAndLetters = arrayOf(
            charArrayOf('a', 'z'), charArrayOf('A', 'Z'), charArrayOf('0', '9')
        )
        private val randomAlphaNumeric: RandomStringGenerator = org.apache.commons.text.RandomStringGenerator.Builder()
            .withinRange(*TestDataFactory.allowedDigitsAndLetters)
            .filteredBy(CharacterPredicates.LETTERS, CharacterPredicates.DIGITS)
            .build()

        fun randomAlphanumeric(length: Int): String {
            return randomAlphaNumeric.generate(length)
        }

        fun aclKey(): AclKey {
            return AclKey(UUID.randomUUID(), UUID.randomUUID())
        }

        fun role(): Role {
            return Role(
                Optional.of(UUID.randomUUID()),
                UUID.randomUUID(),
                rolePrincipal(),
                randomAlphanumeric(5),
                Optional.of<String>(randomAlphanumeric(5))
            )
        }

        fun role(organizationId: UUID): Role {
            return Role(
                Optional.of(UUID.randomUUID()),
                organizationId,
                rolePrincipal(),
                randomAlphanumeric(5),
                Optional.of<String>(randomAlphanumeric(5))
            )
        }

        fun rolePrincipal(): Principal {
            return Principal(
                PrincipalType.ROLE,
                randomAlphanumeric(5)
            )
        }

        fun userPrincipal(): Principal {
            return Principal(PrincipalType.USER, randomAlphanumeric(10))
        }

        fun ace(): Ace {
            return Ace(
                userPrincipal(),
                permissions()
            )
        }

        fun aceValue(): AceValue {
            return AceValue(
                permissions(),
                securableObjectType()
            )
        }

        fun acl(): Acl {
            return Acl(
                AclKey(UUID.randomUUID(), UUID.randomUUID()),
                ImmutableList.of(ace(), ace(), ace(), ace())
            )
        }

        fun aclData(): AclData {
            return AclData(acl(), actions[r.nextInt(actions.size)])
        }

        @JvmStatic
        fun permissions(): EnumSet<Permission> {
            return permissions
                .filter { r.nextBoolean() }
                .toCollection(EnumSet.noneOf(Permission::class.java))
        }

        fun nonEmptyPermissions(): EnumSet<Permission> {
            var ps: EnumSet<Permission> = permissions()
            while (ps.isEmpty()) {
                ps = permissions()
            }
            return ps
        }

        fun securableObjectType(): SecurableObjectType {
            return securableObjectTypes[r.nextInt(securableObjectTypes.size)]

        }

        fun securablePrincipal(type: PrincipalType): SecurablePrincipal {
            val principal: Principal = when (type) {
                PrincipalType.ROLE -> rolePrincipal()
                PrincipalType.ORGANIZATION -> organizationPrincipal()
                PrincipalType.USER -> userPrincipal()
                else -> userPrincipal()
            }
            return SecurablePrincipal(
                AclKey(UUID.randomUUID()),
                principal,
                randomAlphanumeric(10),
                Optional.of<String>(randomAlphanumeric(10))
            )
        }

        fun organizationPrincipal(): Principal {
            return Principal(
                PrincipalType.ORGANIZATION,
                randomAlphanumeric(
                    10
                )
            )
        }

        fun securableOrganizationPrincipal(): OrganizationPrincipal {
            return OrganizationPrincipal(
                Optional.of(UUID.randomUUID()),
                organizationPrincipal(),
                randomAlphanumeric(5),
                Optional.of<String>(randomAlphanumeric(10))
            )
        }
    }
}