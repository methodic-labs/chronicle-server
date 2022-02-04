package com.openlattice.chronicle.study

import com.openlattice.chronicle.ChronicleServerTests
import com.openlattice.chronicle.client.ChronicleClient
import com.openlattice.chronicle.organizations.Organization
import org.junit.Assert
import org.junit.Test

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class OrganizationTests : ChronicleServerTests() {
    private val chronicleClient: ChronicleClient = clientUser1


    @Test
    fun createOrganization() {
        val organizationsApi = chronicleClient.organizationsApi
        val expected = Organization(title = "This is a test study.")
        val orgId =  organizationsApi.createOrganization(expected)
        expected.id = orgId 
        val org = organizationsApi.getOrganization(orgId)
        Assert.assertEquals(expected, org)
    }

}
