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
package com.openlattice.chronicle.pods

import com.auth0.client.mgmt.ManagementAPI
import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.databind.ObjectMapper
import com.geekbeast.hazelcast.HazelcastClientProvider
import com.google.common.eventbus.EventBus
import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.core.HazelcastInstance
import com.kryptnostic.rhizome.configuration.ConfigurationConstants
import com.kryptnostic.rhizome.configuration.service.ConfigurationService
import com.openlattice.auth0.Auth0Pod
import com.openlattice.auth0.Auth0TokenProvider
import com.openlattice.auth0.AwsAuth0TokenProvider
import com.openlattice.authentication.Auth0Configuration
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.HazelcastAuthorizationService
import com.openlattice.chronicle.authorization.principals.HazelcastPrincipalService
import com.openlattice.chronicle.authorization.principals.HazelcastPrincipalsMapManager
import com.openlattice.chronicle.authorization.principals.PrincipalsMapManager
import com.openlattice.chronicle.authorization.principals.SecurePrincipalsManager
import com.openlattice.chronicle.authorization.reservations.AclKeyReservationService
import com.openlattice.chronicle.configuration.ChronicleConfiguration
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.pods.ChronicleServerServicesPod
import com.openlattice.chronicle.serializers.FullQualifiedNameJacksonSerializer.registerWithMapper
import com.openlattice.chronicle.services.ScheduledTasksManager
import com.openlattice.chronicle.services.delete.DataDeletionManager
import com.openlattice.chronicle.services.delete.DataDeletionService
import com.openlattice.chronicle.services.download.DataDownloadManager
import com.openlattice.chronicle.services.download.DataDownloadService
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.services.enrollment.EnrollmentService
import com.openlattice.chronicle.services.settings.OrganizationSettingsManager
import com.openlattice.chronicle.services.settings.OrganizationSettingsService
import com.openlattice.chronicle.services.surveys.SurveysManager
import com.openlattice.chronicle.services.surveys.SurveysService
import com.openlattice.chronicle.services.upload.AppDataUploadManager
import com.openlattice.chronicle.services.upload.AppDataUploadService
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.tasks.PostConstructInitializerTaskDependencies
import com.openlattice.chronicle.users.Auth0SyncInitializationTask
import com.openlattice.jdbc.DataSourceManager
import com.openlattice.users.*
import com.openlattice.users.export.Auth0ApiExtension
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Profile
import java.io.IOException
import java.util.concurrent.ExecutionException
import javax.inject.Inject

@Configuration
@Import(Auth0Pod::class)
class ChronicleServerServicesPod {
    @Inject
    private lateinit var configurationService: ConfigurationService

    @Inject
    private lateinit var auth0Configuration: Auth0Configuration

    @Inject
    private lateinit var dataSourceManager: DataSourceManager

    @Inject
    private lateinit var hazelcastClientProvider: HazelcastClientProvider

    @Inject
    private lateinit var executor: ListeningExecutorService

    @Inject
    private lateinit var hazelcast: HazelcastInstance

    @Inject
    private lateinit var eventBus: EventBus
    @Bean
    fun defaultObjectMapper(): ObjectMapper {
        val mapper = ObjectMappers.getJsonMapper()
        registerWithMapper(mapper)
        return mapper
    }

    @get:Throws(IOException::class)
    @get:Bean(name = ["chronicleConfiguration"])
    val chronicleConfiguration: ChronicleConfiguration
        get() = configurationService.getConfiguration(ChronicleConfiguration::class.java)!!

    @Bean
    fun auth0TokenProvider(): Auth0TokenProvider {
        return AwsAuth0TokenProvider(auth0Configuration)
    }

    @Bean
    @Throws(IOException::class, ExecutionException::class)
    fun scheduledTasksManager(): ScheduledTasksManager {
        return ScheduledTasksManager()
    }

    @Bean(name = ["auth0SyncTask"])
    @Profile(ConfigurationConstants.Profiles.LOCAL_CONFIGURATION_PROFILE)
    fun localAuth0SyncTask(): Auth0SyncTask {
        logger.info("Constructing local auth0sync task")
        return LocalAuth0SyncTask()
    }

    @Bean(name = ["auth0SyncTask"])
    @Profile(
        ConfigurationConstants.Profiles.AWS_CONFIGURATION_PROFILE,
        ConfigurationConstants.Profiles.AWS_TESTING_PROFILE,
        ConfigurationConstants.Profiles.KUBERNETES_CONFIGURATION_PROFILE
    )
    fun defaultAuth0SyncTask(): Auth0SyncTask {
        logger.info("Constructing DEFAULT auth0sync task")
        return DefaultAuth0SyncTask()
    }

    @Bean(name = ["auth0SyncInitializationTask"])
    @Profile(ConfigurationConstants.Profiles.LOCAL_CONFIGURATION_PROFILE)
    fun localAuth0SyncInitializationTask(): Auth0SyncInitializationTask<*> {
        return Auth0SyncInitializationTask(LocalAuth0SyncTask::class.java)
    }

    @Bean(name = ["auth0SyncInitializationTask"])
    @Profile(
        ConfigurationConstants.Profiles.AWS_CONFIGURATION_PROFILE,
        ConfigurationConstants.Profiles.AWS_TESTING_PROFILE,
        ConfigurationConstants.Profiles.KUBERNETES_CONFIGURATION_PROFILE
    )
    fun defaultAuth0SyncInitializationTask(): Auth0SyncInitializationTask<*> {
        return Auth0SyncInitializationTask(
            DefaultAuth0SyncTask::class.java
        )
    }

    @Bean
    @Throws(IOException::class, ExecutionException::class)
    fun dataDeletionManager(): DataDeletionManager {
        return DataDeletionService(enrollmentManager())
    }

    @Bean
    @Throws(IOException::class, ExecutionException::class)
    fun dataDownloadManager(): DataDownloadManager {
        return DataDownloadService()
    }

    @Bean
    @Throws(IOException::class, ExecutionException::class)
    fun enrollmentManager(): EnrollmentManager {
        return EnrollmentService(scheduledTasksManager())
    }

    @Bean
    fun storageResolver(): StorageResolver {
        return StorageResolver(dataSourceManager!!, "")
    }

    @Bean
    fun organizationSettingsManager(): OrganizationSettingsManager {
        return OrganizationSettingsService()
    }

    @Bean
    @Throws(IOException::class, ExecutionException::class)
    fun appDataUploadManager(): AppDataUploadManager {
        return AppDataUploadService(
            storageResolver(),
            scheduledTasksManager(),
            enrollmentManager(),
            organizationSettingsManager()
        )
    }

    @Bean
    @Throws(IOException::class, ExecutionException::class)
    fun surveysManager(): SurveysManager {
        return SurveysService(enrollmentManager())
    }

    @Bean
    fun postConstructInitializerTaskDependencies(): PostConstructInitializerTaskDependencies {
        return PostConstructInitializerTaskDependencies()
    }

    @Bean
    fun postInitializerTask(): PostConstructInitializerTaskDependencies.PostConstructInitializerTask {
        return PostConstructInitializerTaskDependencies.PostConstructInitializerTask()
    }

    @Bean
    fun idGenerationService(): HazelcastIdGenerationService {
        return HazelcastIdGenerationService(hazelcastClientProvider!!)
    }

    @Bean
    fun principalsMapManager(): PrincipalsMapManager {
        return HazelcastPrincipalsMapManager(hazelcast!!, aclKeyReservationService())
    }

    @Bean
    fun aclKeyReservationService(): AclKeyReservationService {
        return AclKeyReservationService(dataSourceManager!!)
    }

    @Bean
    fun authorizationManager(): AuthorizationManager {
        return HazelcastAuthorizationService(hazelcast!!, eventBus!!, principalsMapManager())
    }

    @Bean
    fun principalsManager(): SecurePrincipalsManager {
        return HazelcastPrincipalService(
            hazelcast!!,
            aclKeyReservationService(),
            authorizationManager(),
            principalsMapManager()
        )
    }

    @Bean
    fun auth0SyncService(): Auth0SyncService {
        return Auth0SyncService(hazelcast!!, principalsManager())
    }

    @Bean
    fun userListingService(): UserListingService {
        if (auth0Configuration!!.managementApiUrl.contains(Auth0Configuration.NO_SYNC_URL)) {
            return LocalUserListingService(auth0Configuration)
        }
        val auth0Token = auth0TokenProvider().token
        return Auth0UserListingService(
            ManagementAPI(auth0Configuration.domain, auth0Token),
            Auth0ApiExtension(auth0Configuration.domain, auth0Token)
        )
    }

    @Bean
    fun auth0SyncTaskDependencies(): Auth0SyncTaskDependencies {
        return Auth0SyncTaskDependencies(auth0SyncService(), userListingService(), executor!!)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ChronicleServerServicesPod::class.java)
    }
}