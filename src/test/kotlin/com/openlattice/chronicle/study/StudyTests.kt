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
class StudyTests : ChronicleServerTests() {
    private val chronicleClient: ChronicleClient = clientUser1
    private val chronicleClient2: ChronicleClient = clientUser2

    @Test
    fun createStudy() {
        val studyApi = chronicleClient.studyApi
        val expected = Study(title = "This is a test study.", contact = "test@openlattice.com")
        val studyId = studyApi.createStudy(expected)
        val study = studyApi.getStudy(studyId)
        Assert.assertEquals(studyId, study.id)
    }

    @Test
    fun updateStudy() {
        val studyApi = chronicleClient.studyApi
        val expected = Study(title = "This is a test study.", contact = "test@openlattice.com")
        val studyId = studyApi.createStudy(expected)
        val study = studyApi.getStudy(studyId)
        Assert.assertEquals(studyId, study.id)

        val desc = "Now it has a description"
        studyApi.updateStudy(studyId, StudyUpdate(
            description = desc,
            notificationsEnabled = false
        ))

        val updatedStudy1 = studyApi.getStudy(studyId)

        Assert.assertEquals(studyId, study.id)
        Assert.assertEquals(desc, updatedStudy1.description)
        Assert.assertFalse(updatedStudy1.notificationsEnabled)

        val updatedStudy2 = studyApi.updateStudy(studyId, StudyUpdate(
            notificationsEnabled = true
        ), true)!!

        Assert.assertEquals(studyId, study.id)
        Assert.assertEquals(desc, updatedStudy2.description)
        Assert.assertTrue(updatedStudy2.notificationsEnabled)

    }

    @Test
    fun getOrgStudies() {
        val organization1 = Organization(title = "test org 1")
        val organization2 = Organization(title = "test org 2")
        val organization3 = Organization(title = "test org 3")

        // client 1 is owner of orgs 1 & 2 and study 1 & 2
        // client 2 is owner of org 3 and study 3
        val client1OrgId1 = chronicleClient.organizationsApi.createOrganization(organization1)
        val client1OrgId2 = chronicleClient.organizationsApi.createOrganization(organization2)
        val client2OrgId3 = chronicleClient2.organizationsApi.createOrganization(organization3)

        // study 1 owned by client 1, org 1
        val study1OrgIds = setOf(client1OrgId1)
        val study1Title = "org 1 study 1"
        val expectedStudy1 = Study(
            title = study1Title,
            contact = "test@openlattice.com",
            organizationIds = study1OrgIds
        )
        // Study 2 is owned by client 1, in both org 1 and org 2
        val study2OrgIds = setOf(client1OrgId1, client1OrgId2)
        val study2Title = "org 2 study 2"
        val expectedStudy2 = Study(
            title = study2Title,
            contact = "test@openlattice.com",
            organizationIds = study2OrgIds
        )
        // Study 3 is owned by client 2, org 3
        val study3OrgIds = setOf(client2OrgId3)
        val study3Title = "org 3 study 3"
        val expectedStudy3 = Study(
            title = study3Title,
            contact = "test@openlattice.com",
            organizationIds = setOf(client2OrgId3)
        )

        val study1Id = chronicleClient.studyApi.createStudy(expectedStudy1)
        val study2Id = chronicleClient.studyApi.createStudy(expectedStudy2)
        val study3Id = chronicleClient2.studyApi.createStudy(expectedStudy3)

        val actualOrg1Studies = chronicleClient.studyApi.getOrgStudies(client1OrgId1)
        // API returns list in descending creation times (recent first)
        // expect [study 2, study 1] from org 1
        Assert.assertEquals(2, actualOrg1Studies.size)
        Assert.assertEquals(listOf(study2Id, study1Id), actualOrg1Studies.map { study -> study.id })
        Assert.assertEquals(listOf(study2Title, study1Title), actualOrg1Studies.map { study -> study.title })
        Assert.assertEquals(study2OrgIds, actualOrg1Studies[0].organizationIds)
        Assert.assertEquals(study1OrgIds, actualOrg1Studies[1].organizationIds)

        // expect [study 2] from org 2
        val actualOrg2Studies = chronicleClient.studyApi.getOrgStudies(client1OrgId2)
        Assert.assertEquals(1, actualOrg2Studies.size)
        Assert.assertEquals(listOf(study2Id), actualOrg2Studies.map { study -> study.id }, )
        Assert.assertEquals(listOf(study2Title), actualOrg2Studies.map { study -> study.title })
        Assert.assertEquals(study2OrgIds, actualOrg2Studies[0].organizationIds)

        // expect [study 3] from org 3
        val actualOrg3Studies = chronicleClient2.studyApi.getOrgStudies(client2OrgId3)
        Assert.assertEquals(1, actualOrg3Studies.size)
        Assert.assertEquals(listOf(study3Id), actualOrg3Studies.map { study -> study.id })
        Assert.assertEquals(listOf(study3Title),actualOrg3Studies.map { study -> study.title })
        Assert.assertEquals(study3OrgIds, actualOrg3Studies[0].organizationIds)
    }
}
