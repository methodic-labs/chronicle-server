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
import com.kryptnostic.rhizome.configuration.service.ConfigurationService;
import com.openlattice.auth0.Auth0Pod;
import com.openlattice.auth0.Auth0TokenProvider;
import com.openlattice.auth0.AwsAuth0TokenProvider;
import com.openlattice.authentication.Auth0Configuration;
import com.openlattice.chronicle.configuration.ChronicleConfiguration;
import com.openlattice.chronicle.services.ScheduledTasksManager;
import com.openlattice.chronicle.services.delete.DataDeletionManager;
import com.openlattice.chronicle.services.delete.DataDeletionService;
import com.openlattice.chronicle.services.download.DataDownloadManager;
import com.openlattice.chronicle.services.download.DataDownloadService;
import com.openlattice.chronicle.services.enrollment.EnrollmentManager;
import com.openlattice.chronicle.services.enrollment.EnrollmentService;
import com.openlattice.chronicle.services.surveys.SurveysManager;
import com.openlattice.chronicle.services.surveys.SurveysService;
import com.openlattice.chronicle.services.upload.AppDataUploadManager;
import com.openlattice.chronicle.services.upload.AppDataUploadService;
import com.openlattice.chronicle.storage.StorageResolver;
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer;
import com.openlattice.jdbc.DataSourceManager;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import javax.inject.Inject;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import( {
        Auth0Pod.class,
} )
public class ChronicleServerServicesPod {

    @Inject
    private ConfigurationService configurationService;

    @Inject
    private Auth0Configuration auth0Configuration;

    @Inject
    private DataSourceManager dataSourceManager;

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
    public AppDataUploadManager appDataUploadManager() throws IOException, ExecutionException {
        return new AppDataUploadService( storageResolver(), scheduledTasksManager(), enrollmentManager() );
    }

    @Bean
    public SurveysManager surveysManager() throws IOException, ExecutionException {
        return new SurveysService( enrollmentManager() );
    }
}
