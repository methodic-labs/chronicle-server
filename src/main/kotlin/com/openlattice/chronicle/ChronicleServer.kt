package com.openlattice.chronicle

import com.geekbeast.mappers.mappers.ObjectMappers
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.geekbeast.rhizome.configuration.websockets.BaseRhizomeServer
import com.geekbeast.rhizome.core.RhizomeApplicationServer
import com.geekbeast.rhizome.hazelcast.serializers.RhizomeUtils.Pods
import com.geekbeast.rhizome.pods.hazelcast.RegistryBasedHazelcastInstanceConfigurationPod
import com.geekbeast.auth0.Auth0Pod
import com.geekbeast.aws.AwsS3Pod
import com.openlattice.chronicle.hazelcast.pods.SharedStreamSerializersPod
import com.openlattice.chronicle.mapstores.MapstoresPod
import com.openlattice.chronicle.pods.*
import com.openlattice.chronicle.serializers.FullQualifiedNameJacksonSerializer
import com.openlattice.chronicle.storage.pods.ByteBlobServicePod
import com.openlattice.ioc.providers.LateInitProvidersPod
import com.geekbeast.jdbc.JdbcPod
import com.geekbeast.postgres.PostgresPod
import com.geekbeast.pods.TaskSchedulerPod
import com.openlattice.chronicle.hazelcast.pods.HazelcastQueuePod
import com.openlattice.chronicle.pods.servlet.ChronicleServerSecurityPod
import com.openlattice.chronicle.pods.tables.PostgresDataTablesPod
import com.openlattice.chronicle.pods.tables.PostgresTablesPod
import com.openlattice.chronicle.pods.tables.RedshiftTablesPod

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
            MapstoresPod::class.java,
            RegistryBasedHazelcastInstanceConfigurationPod::class.java,
            Auth0Pod::class.java
        )
        val chronicleServerPods = arrayOf(
            ChronicleConfigurationPod::class.java,
            AwsS3Pod::class.java,
            JdbcPod::class.java,
            ChronicleServerServicesPod::class.java,
            PostgresPod::class.java,
            PostgresTablesPod::class.java,
            RedshiftTablesPod::class.java,
            PostgresDataTablesPod::class.java,
            TaskSchedulerPod::class.java,
            SharedStreamSerializersPod::class.java,
            ByteBlobServicePod::class.java,
            LateInitProvidersPod::class.java,
            HazelcastQueuePod::class.java,
            ChronicleJobRunnersPod::class.java,
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
