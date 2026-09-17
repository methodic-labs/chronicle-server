package com.openlattice.chronicle.users

import com.auth0.json.mgmt.users.User
import com.openlattice.chronicle.authorization.Principal
import com.openlattice.chronicle.authorization.PrincipalType

/**
 * Projects an Auth0 user record down to the directory entry we hand out to study admins.
 *
 * Auth0 models every way a user can sign in as an identity, so a user's login type is the union of the providers
 * backing their identities. A user with no identities -- which happens for a record that was only ever synced by id --
 * is reported as [LoginType.UNKNOWN] rather than guessed at.
 */
fun User.toChronicleUser(): ChronicleUser {
    val identities = this.identities ?: listOf()
    return ChronicleUser(
        principal = Principal(PrincipalType.USER, this.id),
        email = this.email,
        name = this.name ?: this.nickname ?: this.username,
        picture = this.picture,
        emailVerified = this.isEmailVerified ?: false,
        loginType = LoginType.resolve(identities.map { LoginType.fromProvider(it.provider, it.connection) }),
        connections = identities.mapNotNull { it.connection }.distinct().sorted(),
    )
}
