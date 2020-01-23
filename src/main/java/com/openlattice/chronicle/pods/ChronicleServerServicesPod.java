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
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.rhizome.configuration.service.ConfigurationService;
import com.openlattice.auth0.Auth0Pod;
import com.openlattice.auth0.Auth0TokenProvider;
import com.openlattice.authentication.Auth0Configuration;
import com.openlattice.chronicle.configuration.ChronicleConfiguration;
import com.openlattice.chronicle.services.ChronicleService;
import com.openlattice.chronicle.services.ChronicleServiceImpl;
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.inject.Inject;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

@Configuration
@Import( {
        Auth0Pod.class,
} )
public class ChronicleServerServicesPod {

    @Inject
    private HazelcastInstance hazelcastInstance;

    @Inject
    private Auth0Configuration auth0Configuration;

    @Inject
    private ListeningExecutorService executor;

    @Inject
    private ConfigurationService configurationService;

    @Inject
    private EventBus eventBus;

    @Bean
    public ObjectMapper defaultObjectMapper() {
        ObjectMapper mapper = ObjectMappers.getJsonMapper();
        FullQualifiedNameJacksonSerializer.registerWithMapper( mapper );
        return mapper;
    }

    @Bean( name = "chronicleConfiguration" )
    public ChronicleConfiguration getChronicleConfiguration() throws IOException {
        ChronicleConfiguration config = configurationService.getConfiguration( ChronicleConfiguration.class );
        return config;
    }

    @Bean
    public Auth0TokenProvider auth0TokenProvider() {
        // return new Auth0TokenProvider( auth0Configuration );
        return null;
    }

    @Bean
    public ChronicleService chronicleService() throws IOException, ExecutionException {
        return new ChronicleServiceImpl( eventBus, getChronicleConfiguration() );
    }
}
