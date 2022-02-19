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
package com.openlattice.chronicle.directory

import com.auth0.json.mgmt.users.User
import com.openlattice.chronicle.base.OK
import com.openlattice.chronicle.users.Auth0UserBasic
import retrofit2.http.*
import java.lang.Void

// Internal use only! Do NOT add to JDK
interface Auth0ManagementApi {
    @GET(USERS + BASIC_REQUEST_FIELDS)
    fun getAllUsers(@Query(PAGE) page: Int, @Query(PER_PAGE) perPage: Int): Set<Auth0UserBasic>

    @GET("$USERS/$USER_ID_PATH$BASIC_REQUEST_FIELDS")
    fun getUser(@Path(USER_ID) userId: String): Auth0UserBasic

    @PATCH("$USERS/$USER_ID_PATH")
    fun resetRolesOfUser(@Path(USER_ID) userId: String, @Body app_metadata: Map<String?, Any>): OK

    @GET(USERS)
    fun searchAllUsers(
        @Query(QUERY) searchQuery: String,
        @Query(PAGE) page: Int,
        @Query(PER_PAGE) perPage: Int,
        @Query(SEARCH_ENGINE) searchEngine: String
    ): Set<User>

    @DELETE("$USERS/$USER_ID_PATH")
    fun deleteUser(@Path(USER_ID) userId: String): OK

    companion object {
        const val BASIC_REQUEST_FIELDS = "?fields=user_id%2Cemail%2Cnickname%2Capp_metadata"
        const val PAGE = "page"
        const val PER_PAGE = "per_page"
        const val SEARCH_ENGINE = "search_engine"
        const val QUERY = "q"
        const val USERS = "users"
        const val USER_ID = "userId"
        const val USER_ID_PATH = "{$USER_ID}"
    }
}