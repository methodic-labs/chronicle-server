package com.openlattice.chronicle

import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.kryptnostic.rhizome.configuration.websockets.BaseRhizomeServer
import com.kryptnostic.rhizome.core.RhizomeApplicationServer
import com.kryptnostic.rhizome.hazelcast.serializers.RhizomeUtils.Pods
import com.kryptnostic.rhizome.pods.hazelcast.RegistryBasedHazelcastInstanceConfigurationPod
import com.openlattice.auth0.Auth0Pod
import com.openlattice.aws.AwsS3Pod
import com.openlattice.chronicle.hazelcast.pods.SharedStreamSerializersPod
import com.openlattice.chronicle.pods.*
import com.openlattice.chronicle.serializers.FullQualifiedNameJacksonSerializer
import com.openlattice.chronicle.storage.pods.ByteBlobServicePod
import com.openlattice.ioc.providers.LateInitProvidersPod
import com.openlattice.jdbc.JdbcPod
import com.openlattice.postgres.PostgresPod
import com.openlattice.tasks.pods.TaskSchedulerPod

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class ChronicleServer(vararg pods: Class<*>) : BaseRhizomeServer(
        *Pods.concatenate(
                pods,
                webPods,
                rhizomePods,
                RhizomeApplicationServer.DEFAULT_PODS,
                chronicleServerPods
        )
) {
    companion object {
        val webPods = arrayOf(
                ChronicleServerServletsPod::class.java,
                ChronicleServerSecurityPod::class.java
        )
        val rhizomePods = arrayOf(
                RegistryBasedHazelcastInstanceConfigurationPod::class.java,
                Auth0Pod::class.java
        )
        val chronicleServerPods = arrayOf(
            AwsS3Pod::class.java,
            JdbcPod::class.java,
            ChronicleServerServicesPod::class.java,
            PostgresPod::class.java,
            PostgresTablesPod::class.java,
            RedshiftTablesPod::class.java,
            TaskSchedulerPod::class.java,
            SharedStreamSerializersPod::class.java,
            ByteBlobServicePod::class.java,
            LateInitProvidersPod::class.java
        )

        @Throws(Exception::class)
        @JvmStatic
        fun main(args: Array<String>) {
            val chronicleServer = ChronicleServer()
            chronicleServer.start(*args)
        }

        init {
            ObjectMappers.foreach { mapper: ObjectMapper ->
                FullQualifiedNameJacksonSerializer.registerWithMapper(
                        mapper
                )
            }
            ObjectMappers.foreach { mapper: ObjectMapper ->
                mapper.disable(
                        SerializationFeature.WRITE_DATES_AS_TIMESTAMPS
                )
            }
        }
    }

    @Throws(Exception::class)
    override fun start(vararg profiles: String) {
        super.start(*profiles)
    }
}
