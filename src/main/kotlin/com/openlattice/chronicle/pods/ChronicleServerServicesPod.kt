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

import com.fasterxml.jackson.databind.ObjectMapper
import com.geekbeast.auth0.*
import com.geekbeast.authentication.Auth0Configuration
import com.geekbeast.hazelcast.HazelcastClientProvider
import com.geekbeast.jdbc.DataSourceManager
import com.geekbeast.mail.MailService
import com.geekbeast.mail.MailServiceConfig
import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.rhizome.configuration.ConfigurationConstants
import com.geekbeast.rhizome.configuration.service.ConfigurationService
import com.google.common.eventbus.EventBus
import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.core.HazelcastInstance
import com.openlattice.chronicle.auditing.AuditingManager
import com.openlattice.chronicle.auditing.RedshiftAuditingManager
import com.openlattice.chronicle.authorization.AuthorizationManager
import com.openlattice.chronicle.authorization.HazelcastAuthorizationService
import com.openlattice.chronicle.authorization.initializers.AuthorizationInitializationDependencies
import com.openlattice.chronicle.authorization.initializers.AuthorizationInitializationTask
import com.openlattice.chronicle.authorization.principals.*
import com.openlattice.chronicle.authorization.reservations.AclKeyReservationService
import com.openlattice.chronicle.configuration.ChronicleConfiguration
import com.openlattice.chronicle.configuration.TwilioConfiguration
import com.openlattice.chronicle.directory.Auth0UserDirectoryService
import com.openlattice.chronicle.directory.LocalUserDirectoryService
import com.openlattice.chronicle.directory.UserDirectoryService
import com.openlattice.chronicle.ids.HazelcastIdGenerationService
import com.openlattice.chronicle.organizations.ChronicleOrganizationService
import com.openlattice.chronicle.organizations.initializers.OrganizationsInitializationDependencies
import com.openlattice.chronicle.organizations.initializers.OrganizationsInitializationTask
import com.openlattice.chronicle.serializers.FullQualifiedNameJacksonSerializer.registerWithMapper
import com.openlattice.chronicle.services.ScheduledTasksManager
import com.openlattice.chronicle.services.candidates.CandidateService
import com.openlattice.chronicle.services.delete.DataDeletionManager
import com.openlattice.chronicle.services.delete.DataDeletionService
import com.openlattice.chronicle.services.download.DataDownloadManager
import com.openlattice.chronicle.services.download.DataDownloadService
import com.openlattice.chronicle.services.enrollment.EnrollmentManager
import com.openlattice.chronicle.services.enrollment.EnrollmentService
import com.openlattice.chronicle.services.jobs.JobService
import com.openlattice.chronicle.services.notifications.NotificationService
import com.openlattice.chronicle.services.settings.OrganizationSettingsManager
import com.openlattice.chronicle.services.settings.OrganizationSettingsService
import com.openlattice.chronicle.services.studies.*
import com.openlattice.chronicle.services.studies.tasks.StudyComplianceHazelcastTask
import com.openlattice.chronicle.services.studies.tasks.StudyComplianceHazelcastTaskDependencies
import com.openlattice.chronicle.services.surveys.SurveysManager
import com.openlattice.chronicle.services.surveys.SurveysService
import com.openlattice.chronicle.services.timeusediary.TimeUseDiaryService
import com.openlattice.chronicle.services.twilio.TwilioService
import com.openlattice.chronicle.services.upload.AppDataUploadManager
import com.openlattice.chronicle.services.upload.AppDataUploadService
import com.openlattice.chronicle.services.upload.SensorDataUploadService
import com.openlattice.chronicle.storage.StorageResolver
import com.openlattice.chronicle.storage.tasks.*
import com.openlattice.chronicle.studies.tasks.StudyLimitsEnforcementTask
import com.openlattice.chronicle.studies.tasks.StudyLimitsEnforcementTaskDependencies
import com.openlattice.chronicle.study.StudyComplianceManager
import com.openlattice.chronicle.tasks.PostConstructInitializerTaskDependencies
import com.openlattice.chronicle.users.*
import com.openlattice.chronicle.users.export.Auth0ApiExtension
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Profile
import java.io.IOException
import java.util.concurrent.ExecutionException
import javax.annotation.PostConstruct
import javax.inject.Inject

@Configuration
@Import(Auth0Pod::class)
class ChronicleServerServicesPod {
    @Inject
    private lateinit var auth0Configuration: Auth0Configuration

    @Inject
    private lateinit var configurationService: ConfigurationService

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

    @Inject
    private lateinit var chronicleConfiguration: ChronicleConfiguration

    @Inject
    private lateinit var storageResolver: StorageResolver

    @Inject
    private lateinit var twilioConfiguration: TwilioConfiguration

    @Inject
    private lateinit var mailServiceConfig: MailServiceConfig

    @Bean
    fun defaultObjectMapper(): ObjectMapper {
        val mapper = ObjectMappers.getJsonMapper()
        registerWithMapper(mapper)
        return mapper
    }

    @Bean
    fun auth0TokenProvider(): Auth0TokenProvider {
        //TODO: Remove AWS from the name of this class.
        return if (auth0Configuration.managementApiUrl.contains("localhost")) {
            EmptyAuth0TokenProvider(auth0Configuration)
        } else {
            RefreshingAuth0TokenProvider(auth0Configuration)
        }
    }

    @Bean
    fun managementApiProvider(): ManagementApiProvider {
        return ManagementApiProvider(auth0TokenProvider(), auth0Configuration)
    }

    @Bean
    @Throws(IOException::class, ExecutionException::class)
    fun scheduledTasksManager(): ScheduledTasksManager {
        return ScheduledTasksManager(
            storageResolver
        )
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
        return DataDownloadService(storageResolver)
    }

    @Bean
    @Throws(IOException::class, ExecutionException::class)
    fun enrollmentManager(): EnrollmentManager {
        return EnrollmentService(
            storageResolver,
            idGenerationService(),
            candidateService(),
        )
    }


    @Bean
    fun organizationSettingsManager(): OrganizationSettingsManager {
        return OrganizationSettingsService(storageResolver)
    }

    @Bean
    @Throws(IOException::class, ExecutionException::class)
    fun appDataUploadManager(): AppDataUploadManager {
        return AppDataUploadService(
            storageResolver,
            enrollmentManager(),
            studyService()
        )
    }

    @Bean
    @Throws(IOException::class, ExecutionException::class)
    fun surveysManager(): SurveysManager {
        return SurveysService(
            hazelcast,
            storageResolver,
            enrollmentManager(),
            scheduledTasksManager(),
            auditingManager(),
            idGenerationService(),
        )
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
        return HazelcastIdGenerationService(hazelcastClientProvider)
    }

    @Bean
    fun principalsMapManager(): PrincipalsMapManager {
        return HazelcastPrincipalsMapManager(hazelcast, aclKeyReservationService())
    }

    @Bean
    fun aclKeyReservationService(): AclKeyReservationService {
        return AclKeyReservationService(storageResolver)
    }

    @Bean
    fun authorizationService(): AuthorizationManager {
        return HazelcastAuthorizationService(hazelcast, storageResolver, eventBus, principalsMapManager())
    }

    @Bean
    fun principalsManager(): SecurePrincipalsManager {
        return HazelcastPrincipalService(
            hazelcast,
            aclKeyReservationService(),
            authorizationService(),
            principalsMapManager(),
            auditingManager()
        )
    }

    @Bean
    fun mailService(): MailService {
        return MailService(mailServiceConfig)
    }

    @Bean
    fun auth0SyncService(): Auth0SyncService {
        return Auth0SyncService(hazelcast, principalsManager())
    }

    @Bean
    fun userListingService(): UserListingService {
        if (auth0Configuration.managementApiUrl.contains(Auth0Configuration.NO_SYNC_URL)) {
            return LocalUserListingService(auth0Configuration)
        }
        val atp = auth0TokenProvider()
        return Auth0UserListingService(
            managementApiProvider(),
            Auth0ApiExtension(auth0Configuration.domain) { atp.token }
        )
    }


    @Bean
    fun userDirectoryService(): UserDirectoryService {
        return if (auth0Configuration.managementApiUrl.contains(Auth0Configuration.NO_SYNC_URL)) {
            LocalUserDirectoryService(auth0Configuration)
        } else {
            Auth0UserDirectoryService(auth0TokenProvider(), hazelcast)
        }
    }

    @Bean
    fun jobService(): JobService {
        return JobService(
            idGenerationService(),
            storageResolver,
            auditingManager()
        )
    }

    @Bean
    fun studyService(): StudyService {
        return StudyService(
            storageResolver,
            authorizationService(),
            candidateService(),
            enrollmentManager(),
            surveysManager(),
            idGenerationService(),
            studyLimitsManager(),
            auditingManager(),
            hazelcast
        )
    }

    @Bean
    fun twilioService(): TwilioService {
        return TwilioService(
            twilioConfiguration,
            studyService()
        )
    }

    @Bean
    fun notificationService(): NotificationService {
        return NotificationService(
            storageResolver,
            authorizationService(),
            enrollmentManager(),
            candidateService(),
            studyService(),
            jobService(),
            idGenerationService(),
            twilioService(),
            auditingManager(),
        )
    }

    @Bean
    fun timeUseDiaryService(): TimeUseDiaryService {
        return TimeUseDiaryService(storageResolver, idGenerationService(), studyService())
    }

    @Bean
    fun auth0SyncTaskDependencies(): Auth0SyncTaskDependencies {
        return Auth0SyncTaskDependencies(auth0SyncService(), userListingService(), executor)
    }

    @Bean
    fun auth0SyncInitializationTask(): Auth0SyncInitializationTask<Auth0SyncTask> {
        return Auth0SyncInitializationTask(Auth0SyncTask::class.java)
    }

    @Bean
    fun auditingManager(): AuditingManager {
        return RedshiftAuditingManager(storageResolver)
    }

    @Bean
    fun organizationsService(): ChronicleOrganizationService {
        return ChronicleOrganizationService(
            storageResolver,
            authorizationService(),
            idGenerationService(),
            auditingManager()
        )
    }

    @Bean
    fun authorizationInitializationTask(): AuthorizationInitializationTask {
        return AuthorizationInitializationTask()
    }

    @Bean
    fun authorizationInitializationTaskDependencies(): AuthorizationInitializationDependencies {
        return AuthorizationInitializationDependencies(principalsManager())
    }

    @Bean
    fun organizationInitTask(): OrganizationsInitializationTask {
        return OrganizationsInitializationTask()
    }

    @Bean
    fun orgInitTaskDependencies(): OrganizationsInitializationDependencies {
        return OrganizationsInitializationDependencies(
            storageResolver,
            organizationsService(),
            principalsManager(),
            chronicleConfiguration
        )
    }

    @Bean
    fun candidateService(): CandidateService {
        return CandidateService(storageResolver, authorizationService(), idGenerationService())
    }

    @Bean
    fun sensorDataUploadService(): SensorDataUploadService {
        return SensorDataUploadService(storageResolver, studyService())
    }

    @Bean
    fun studyLimitsManager(): StudyLimitsManager {
        return StudyLimitsService(storageResolver, hazelcast)
    }

    @Bean
    fun studyLimitsEnforcementTask(): StudyLimitsEnforcementTask {
        return StudyLimitsEnforcementTask()
    }

    @Bean
    fun studyLimitsEnforcementTaskDependencies(): StudyLimitsEnforcementTaskDependencies {
        return StudyLimitsEnforcementTaskDependencies(
            storageResolver,
            studyLimitsManager(),
            studyService()
        )
    }

    @Bean
    fun studyComplianceManager(): StudyComplianceManager {
        return StudyComplianceService(storageResolver, auditingManager(), hazelcast)
    }

    @Bean
    fun studyComplianceTask(): StudyComplianceHazelcastTask {
        return StudyComplianceHazelcastTask()
    }

    @Bean
    fun studyComplianceTaskDependencies(): StudyComplianceHazelcastTaskDependencies {
        return StudyComplianceHazelcastTaskDependencies(
            studyComplianceManager(),
            studyService(),
            storageResolver,
            notificationService()
        )
    }

    @Bean
    fun moveToEventStorageTaskDependencies(): MoveToEventStorageTaskDependencies {
        return MoveToEventStorageTaskDependencies(storageResolver, studyService())
    }

    @Bean
    fun moveIosDataToEventStorageTaskDependencies(): MoveToIosEventStorageTask {
        return MoveToIosEventStorageTask()
    }

    @Bean
    fun moveToEventStorageTask(): MoveToEventStorageTask {
        return MoveToEventStorageTask()
    }

    @Bean
    fun recalculateParticipantStatsTaskDependencies(): RecalculateParticipantStatsTaskDependencies {
        return RecalculateParticipantStatsTaskDependencies(storageResolver, studyService())
    }

    @Bean
    fun recalculateParticipantStatsTask(): RecalculateParticipantStatsTask {
        return RecalculateParticipantStatsTask()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ChronicleServerServicesPod::class.java)
    }

    @PostConstruct
    fun init() {
        Principals.init(principalsManager(), hazelcast)
        storageResolver.setStudyStorage(hazelcast)
    }
}

