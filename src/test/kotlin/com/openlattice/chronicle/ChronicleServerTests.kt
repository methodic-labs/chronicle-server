package com.openlattice.chronicle

import com.hazelcast.core.HazelcastInstance
import com.geekbeast.rhizome.configuration.ConfigurationConstants
import com.geekbeast.rhizome.core.RhizomeApplicationServer
import com.geekbeast.rhizome.hazelcast.serializers.RhizomeUtils
import com.openlattice.chronicle.constants.ChronicleProfiles
import com.openlattice.chronicle.storage.PostgresDataTables
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.jdbc.DataSourceManager
import com.geekbeast.postgres.PostgresPod
import com.zaxxer.hikari.HikariDataSource

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
open class ChronicleServerTests {
    companion object {
        private val LOCAL_TEST_PROFILES = arrayOf(
            ConfigurationConstants.Profiles.LOCAL_CONFIGURATION_PROFILE,
            PostgresDataTables.POSTGRES_DATA_ENVIRONMENT,
            PostgresPod.PROFILE,
            ChronicleProfiles.MEDIA_LOCAL_PROFILE)
        private val AWS_TEST_PROFILES = arrayOf(
            ConfigurationConstants.Profiles.AWS_TESTING_PROFILE,
            PostgresDataTables.POSTGRES_DATA_ENVIRONMENT,
            PostgresPod.PROFILE,
            ChronicleProfiles.MEDIA_LOCAL_PROFILE
        )

        @JvmField
        val testServer = RhizomeApplicationServer(
            *RhizomeUtils.Pods.concatenate(
                ChronicleServer.webPods,
                ChronicleServer.rhizomePods,
                RhizomeApplicationServer.DEFAULT_PODS,
                ChronicleServer.chronicleServerPods
            )
        )


        @JvmField
        val hazelcastInstance: HazelcastInstance

        @JvmField
        val hds: HikariDataSource

        @JvmField
        val sr: StorageResolver

        @JvmField
        val dsm: DataSourceManager

        init {
            testServer.sprout(*LOCAL_TEST_PROFILES)

            hazelcastInstance = testServer.context.getBean(HazelcastInstance::class.java)
            //This should work as tests aren't sharded all will all share the default datasource
            hds = testServer.context.getBean(HikariDataSource::class.java)
            sr = testServer.context.getBean(StorageResolver::class.java)
            dsm = testServer.context.getBean(DataSourceManager::class.java)
        }
    }
}