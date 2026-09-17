package com.openlattice.chronicle.users

import com.auth0.json.mgmt.users.User
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.openlattice.chronicle.authorization.PrincipalType
import org.junit.Assert
import org.junit.Test

/**
 * Auth0's [User] has no setters for identities, so these build users the way the directory does -- by deserializing a
 * management API payload.
 */
class ChronicleUsersTest {

    private val mapper = ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    private fun user(json: String): User = mapper.readValue(json, User::class.java)

    @Test
    fun testPasswordUser() {
        val chronicleUser = user(
            """
            {
              "user_id": "auth0|1234",
              "email": "jane@example.com",
              "email_verified": true,
              "name": "Jane Doe",
              "picture": "https://example.com/jane.png",
              "identities": [
                { "provider": "auth0", "connection": "Username-Password-Authentication", "user_id": "1234" }
              ]
            }
            """.trimIndent()
        ).toChronicleUser()

        Assert.assertEquals(PrincipalType.USER, chronicleUser.principal.type)
        Assert.assertEquals("auth0|1234", chronicleUser.principal.id)
        Assert.assertEquals("jane@example.com", chronicleUser.email)
        Assert.assertEquals("Jane Doe", chronicleUser.name)
        Assert.assertEquals("https://example.com/jane.png", chronicleUser.picture)
        Assert.assertTrue(chronicleUser.emailVerified)
        Assert.assertEquals(LoginType.USERNAME_PASSWORD, chronicleUser.loginType)
        Assert.assertEquals(listOf("Username-Password-Authentication"), chronicleUser.connections)
    }

    @Test
    fun testSocialUser() {
        val chronicleUser = user(
            """
            {
              "user_id": "google-oauth2|5678",
              "email": "john@example.com",
              "nickname": "john",
              "identities": [
                { "provider": "google-oauth2", "connection": "google-oauth2", "user_id": "5678", "isSocial": true }
              ]
            }
            """.trimIndent()
        ).toChronicleUser()

        Assert.assertEquals(LoginType.OAUTH, chronicleUser.loginType)
        // Falls back to the nickname when Auth0 has no name for the user.
        Assert.assertEquals("john", chronicleUser.name)
        Assert.assertFalse(chronicleUser.emailVerified)
    }

    @Test
    fun testLinkedIdentitiesReportThePasswordLogin() {
        val chronicleUser = user(
            """
            {
              "user_id": "auth0|9999",
              "email": "linked@example.com",
              "identities": [
                { "provider": "auth0", "connection": "Username-Password-Authentication", "user_id": "9999" },
                { "provider": "google-oauth2", "connection": "google-oauth2", "user_id": "5678" }
              ]
            }
            """.trimIndent()
        ).toChronicleUser()

        Assert.assertEquals(LoginType.USERNAME_PASSWORD, chronicleUser.loginType)
        Assert.assertEquals(
            listOf("Username-Password-Authentication", "google-oauth2"),
            chronicleUser.connections
        )
    }

    @Test
    fun testUserWithoutIdentities() {
        val chronicleUser = user(
            """{ "user_id": "auth0|0000", "email": "nobody@example.com" }"""
        ).toChronicleUser()

        Assert.assertEquals(LoginType.UNKNOWN, chronicleUser.loginType)
        Assert.assertTrue(chronicleUser.connections.isEmpty())
        // Nothing to fall back to for a display name, so it stays null rather than echoing the email back.
        Assert.assertNull(chronicleUser.name)
    }
}
