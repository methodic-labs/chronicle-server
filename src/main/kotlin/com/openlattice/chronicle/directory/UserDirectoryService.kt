/*
 * Copyright (C) 2019. OpenLattice, Inc.
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
 *
 */

package com.openlattice.chronicle.directory

import com.auth0.json.mgmt.users.User
import com.codahale.metrics.annotation.Timed
import com.openlattice.chronicle.users.Auth0UserSearchFields

internal const val DEFAULT_PAGE_SIZE = 100
internal const val SEARCH_ENGINE_VERSION = "v3"

/**
 * Shortest email prefix that may be searched for. Anything shorter matches too much of the directory to be a search.
 */
const val MIN_EMAIL_SEARCH_LENGTH = 2

/**
 * Cap on how many users a single directory search will return. A study admin picks one person out of the results, so
 * a result set larger than this means the query needs to be narrowed rather than paged through.
 */
const val MAX_SEARCH_RESULTS = 50

interface UserDirectoryService {

    @Timed
    fun getAllUsers(): Map<String, User>

    @Timed
    fun getUser(userId: String): User

    @Timed
    fun getUsers(userIds: Set<String>): Map<String, User>

    //TODO: Switch over to a Hazelcast map to relieve pressure from Auth0
    @Timed
    fun searchAllUsers(fields: Auth0UserSearchFields): Map<String, User>

    /**
     * Finds users whose email address starts with [emailPrefix], case insensitively.
     *
     * This backs "find the person I want to grant access to" flows, where the caller has typed part of an email
     * address rather than a whole one. [searchAllUsers] is an exact match search and is kept as-is for callers that
     * already know the full address.
     *
     * @param emailPrefix At least [MIN_EMAIL_SEARCH_LENGTH] characters, so that a search can't be used to walk the
     * whole directory.
     * @return The matching users, keyed by user id, capped at [MAX_SEARCH_RESULTS].
     */
    @Timed
    fun searchUsersByEmail(emailPrefix: String): Map<String, User>

    @Timed
    fun deleteUser(userId: String)
}




