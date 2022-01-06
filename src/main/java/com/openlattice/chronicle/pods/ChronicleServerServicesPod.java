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

package com.openlattice.chronicle.pods;

import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.geekbeast.hazelcast.HazelcastClientProvider;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.rhizome.configuration.ConfigurationConstants;
import com.kryptnostic.rhizome.configuration.service.ConfigurationService;
import com.openlattice.auth0.Auth0Pod;
import com.openlattice.auth0.Auth0TokenProvider;
import com.openlattice.auth0.AwsAuth0TokenProvider;
import com.openlattice.authentication.Auth0Configuration;
import com.openlattice.chronicle.authorization.principals.HazelcastPrincipalService;
import com.openlattice.chronicle.authorization.principals.SecurePrincipalsManager;
import com.openlattice.chronicle.configuration.ChronicleConfiguration;
import com.openlattice.chronicle.ids.HazelcastIdGenerationService;
import com.openlattice.chronicle.serializers.FullQualifiedNameJacksonSerializer;
import com.openlattice.chronicle.services.ScheduledTasksManager;
import com.openlattice.chronicle.services.delete.DataDeletionManager;
import com.openlattice.chronicle.services.delete.DataDeletionService;
import com.openlattice.chronicle.services.download.DataDownloadManager;
import com.openlattice.chronicle.services.download.DataDownloadService;
import com.openlattice.chronicle.services.enrollment.EnrollmentManager;
import com.openlattice.chronicle.services.enrollment.EnrollmentService;
import com.openlattice.chronicle.services.settings.OrganizationSettingsManager;
import com.openlattice.chronicle.services.settings.OrganizationSettingsService;
import com.openlattice.chronicle.services.surveys.SurveysManager;
import com.openlattice.chronicle.services.surveys.SurveysService;
import com.openlattice.chronicle.services.upload.AppDataUploadManager;
import com.openlattice.chronicle.services.upload.AppDataUploadService;
import com.openlattice.chronicle.storage.ByteBlobDataManager;
import com.openlattice.chronicle.storage.StorageResolver;
import com.openlattice.chronicle.tasks.PostConstructInitializerTaskDependencies;
import com.openlattice.chronicle.tasks.PostConstructInitializerTaskDependencies.PostConstructInitializerTask;
import com.openlattice.chronicle.users.Auth0SyncInitializationTask;
import com.openlattice.jdbc.DataSourceManager;
import com.openlattice.users.Auth0SyncService;
import com.openlattice.users.Auth0SyncTask;
import com.openlattice.users.Auth0SyncTaskDependencies;
import com.openlattice.users.DefaultAuth0SyncTask;
import com.openlattice.users.LocalAuth0SyncTask;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

@Configuration
@Import( {
        Auth0Pod.class,
} )
public class ChronicleServerServicesPod {
    private static final Logger               logger = LoggerFactory.getLogger( ChronicleServerServicesPod.class );
    @Inject
    private              ConfigurationService configurationService;

    @Inject
    private Auth0Configuration auth0Configuration;

    @Inject
    private DataSourceManager dataSourceManager;

    @Inject
    private HazelcastClientProvider hazelcastClientProvider;

    @Inject
    private ListeningExecutorService executor;

    @Inject
    private HazelcastInstance hazelcast;

    @Bean
    public ObjectMapper defaultObjectMapper() {
        ObjectMapper mapper = ObjectMappers.getJsonMapper();
        FullQualifiedNameJacksonSerializer.registerWithMapper( mapper );
        return mapper;
    }

    @Bean( name = "chronicleConfiguration" )
    public ChronicleConfiguration getChronicleConfiguration() throws IOException {
        return configurationService.getConfiguration( ChronicleConfiguration.class );
    }

    @Bean
    public Auth0TokenProvider auth0TokenProvider() {
        return new AwsAuth0TokenProvider( auth0Configuration );
    }

    @Bean
    public ScheduledTasksManager scheduledTasksManager() throws IOException, ExecutionException {
        return new ScheduledTasksManager();
    }

    @Bean( name = "auth0SyncTask" )
    @Profile( { ConfigurationConstants.Profiles.LOCAL_CONFIGURATION_PROFILE } )
    public Auth0SyncTask localAuth0SyncTask() {
        logger.info( "Constructing local auth0sync task" );
        return new LocalAuth0SyncTask();
    }

    @Bean( name = "auth0SyncTask" )
    @Profile( {
            ConfigurationConstants.Profiles.AWS_CONFIGURATION_PROFILE,
            ConfigurationConstants.Profiles.AWS_TESTING_PROFILE,
            ConfigurationConstants.Profiles.KUBERNETES_CONFIGURATION_PROFILE
    } )
    public Auth0SyncTask defaultAuth0SyncTask() {
        logger.info( "Constructing DEFAULT auth0sync task" );
        return new DefaultAuth0SyncTask();
    }


    @Bean( name = "auth0SyncInitializationTask" )
    @Profile( { ConfigurationConstants.Profiles.LOCAL_CONFIGURATION_PROFILE } )
    public Auth0SyncInitializationTask localAuth0SyncInitializationTask() {
        return new Auth0SyncInitializationTask<LocalAuth0SyncTask>( LocalAuth0SyncTask.class );
    }

    @Bean( name = "auth0SyncInitializationTask" )
    @Profile( {
            ConfigurationConstants.Profiles.AWS_CONFIGURATION_PROFILE,
            ConfigurationConstants.Profiles.AWS_TESTING_PROFILE,
            ConfigurationConstants.Profiles.KUBERNETES_CONFIGURATION_PROFILE
    } )
    public Auth0SyncInitializationTask defaultAuth0SyncInitializationTask() {
        return new Auth0SyncInitializationTask<DefaultAuth0SyncTask>( DefaultAuth0SyncTask.class );
    }



    @Bean
    public DataDeletionManager dataDeletionManager() throws IOException, ExecutionException {
        return new DataDeletionService( enrollmentManager() );
    }

    @Bean
    public DataDownloadManager dataDownloadManager() throws IOException, ExecutionException {
        return new DataDownloadService();
    }

    @Bean
    public EnrollmentManager enrollmentManager() throws IOException, ExecutionException {
        return new EnrollmentService( scheduledTasksManager() );
    }

    @Bean
    public StorageResolver storageResolver() {
        return new StorageResolver( dataSourceManager, "" );
    }

    @Bean
    public OrganizationSettingsManager organizationSettingsManager() {
        return new OrganizationSettingsService();
    }

    @Bean
    public AppDataUploadManager appDataUploadManager() throws IOException, ExecutionException {
        return new AppDataUploadService( storageResolver(),
                scheduledTasksManager(),
                enrollmentManager(),
                organizationSettingsManager() );
    }

    @Bean
    public SurveysManager surveysManager() throws IOException, ExecutionException {
        return new SurveysService( enrollmentManager() );
    }

    @Bean
    public PostConstructInitializerTaskDependencies postConstructInitializerTaskDependencies() {
        return new PostConstructInitializerTaskDependencies();
    }

    @Bean
    public PostConstructInitializerTask postInitializerTask() {
        return new PostConstructInitializerTask();
    }

    @Bean
    public HazelcastIdGenerationService idGenerationService() {
        return new HazelcastIdGenerationService( hazelcastClientProvider );
    }

    @Bean
    public SecurePrincipalsManager principalsManager() {
        return new HazelcastPrincipalService( hazelcast,
                aclK)
    }

    @Bean
    public Auth0SyncService auth0SyncService() {
        return new Auth0SyncService( hazelcast, principalsManager() );
    }

    @Bean
    public Auth0SyncTaskDependencies auth0SyncTaskDependencies() {
        return new Auth0SyncTaskDependencies( auth0SyncService(),
                userListingService() ,
                executor
        );
    }
}
