package com.openlattice.chronicle

import com.hazelcast.core.HazelcastInstance
import com.kryptnostic.rhizome.configuration.ConfigurationConstants
import com.kryptnostic.rhizome.core.RhizomeApplicationServer
import com.kryptnostic.rhizome.hazelcast.serializers.RhizomeUtils
import com.openlattice.chronicle.constants.ChronicleProfiles
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.postgres.PostgresPod
import com.zaxxer.hikari.HikariDataSource

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class ChronicleServerTests {
    companion object {

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

        init {
            testServer.sprout(
                ConfigurationConstants.Profiles.LOCAL_CONFIGURATION_PROFILE,
                PostgresPod.PROFILE,
                ChronicleProfiles.MEDIA_LOCAL_PROFILE
            )

            hazelcastInstance = testServer.context.getBean(HazelcastInstance::class.java)
            //This should work as tests aren't sharded all will all share the default datasource
            hds = testServer.context.getBean(HikariDataSource::class.java)
            sr = testServer.context.getBean(StorageResolver::class.java)
        }
    }
}