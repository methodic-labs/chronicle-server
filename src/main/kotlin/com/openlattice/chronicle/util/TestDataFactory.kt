package com.openlattice.chronicle.util

import com.openlattice.chronicle.authorization.AclKey
import com.openlattice.chronicle.authorization.Principal
import com.openlattice.chronicle.authorization.PrincipalType
import com.openlattice.chronicle.authorization.Role
import org.apache.commons.text.CharacterPredicates
import org.apache.commons.text.RandomStringGenerator
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class TestDataFactory {
    companion object {
        private val allowedDigitsAndLetters = arrayOf(
                charArrayOf('a', 'z'), charArrayOf('A', 'Z'), charArrayOf('0', '9')
        )
        private val randomAlphaNumeric: RandomStringGenerator = org.apache.commons.text.RandomStringGenerator.Builder()
                .withinRange(*TestDataFactory.allowedDigitsAndLetters)
                .filteredBy(CharacterPredicates.LETTERS, CharacterPredicates.DIGITS)
                .build()

        fun randomAlphanumeric( length : Int ) : String {
            return randomAlphaNumeric.generate( length );
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
    }
}