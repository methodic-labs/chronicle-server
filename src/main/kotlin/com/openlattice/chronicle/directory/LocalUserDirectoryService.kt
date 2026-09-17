package com.openlattice.chronicle.directory

import com.auth0.json.mgmt.users.User
import com.geekbeast.authentication.Auth0Configuration
import com.openlattice.chronicle.users.Auth0UserSearchFields
import org.springframework.stereotype.Service

@Service
class LocalUserDirectoryService(auth0Configuration: Auth0Configuration) : UserDirectoryService {

    val users = auth0Configuration.users.associateBy { it.id }.toMutableMap()

    override fun getAllUsers(): Map<String, User> {
        return users
    }

    override fun getUser(userId: String): User {
        return users.getValue(userId)
    }

    override fun getUsers(userIds: Set<String>): Map<String, User> {
        // Mirrors the Auth0 backed implementation, which drops ids it has no record for rather than throwing. A
        // principal can outlive its directory entry, and callers resolving a stale one shouldn't fail the request.
        return userIds.mapNotNull { id -> users[id]?.let { id to it } }.toMap()
    }

    override fun searchAllUsers(fields: Auth0UserSearchFields): Map<String, User> {
        val email = fields.email ?: ""
        val name = fields.name ?: ""
        return users.values.filter { user ->
            (listOf(user.email, user.name, user.nickname, user.givenName, user.familyName, user.username)
                    + user.identities.map { it.userId }
                    + user.identities.map { it.connection })
                .any { email.contains(it) || name.contains(it) }
        }.associateBy { it.id }
    }

    override fun searchUsersByEmail(emailPrefix: String): Map<String, User> {
        val trimmed = emailPrefix.trim()
        require(trimmed.length >= MIN_EMAIL_SEARCH_LENGTH) {
            "An email search requires at least $MIN_EMAIL_SEARCH_LENGTH characters."
        }
        return users.values
            .filter { it.email?.startsWith(trimmed, ignoreCase = true) == true }
            .sortedBy { it.email }
            .take(MAX_SEARCH_RESULTS)
            .associateBy { it.id }
    }

    override fun deleteUser(userId: String) {
        users.remove(userId)
    }
}
