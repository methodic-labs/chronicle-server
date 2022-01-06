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
 *
 */

package com.openlattice.chronicle.storage.pods;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.kryptnostic.rhizome.configuration.ConfigurationConstants.Profiles;
import com.kryptnostic.rhizome.pods.ConfigurationLoader;
import com.openlattice.chronicle.configuration.ChronicleConfiguration;
import com.openlattice.chronicle.constants.ChronicleProfiles;
import com.openlattice.chronicle.storage.ByteBlobDataManager;
import com.openlattice.chronicle.storage.aws.AwsBlobDataService;
import com.openlattice.chronicle.storage.local.LocalBlobDataService;
import com.zaxxer.hikari.HikariDataSource;
import javax.inject.Inject;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Profile;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Configuration
public class ByteBlobServicePod {
    @Inject
    private HikariDataSource hikariDataSource;

    @Inject
    private ListeningExecutorService executorService;

    @Inject
    private ConfigurationLoader configurationLoader;

    @Bean
    public ChronicleConfiguration chronicleConfiguration() {
        return configurationLoader.logAndLoad( "chronicle", ChronicleConfiguration.class );
    }

    @Bean( name = "byteBlobDataManager" )
    @DependsOn( "chronicleConfiguration" )
    @Profile( { ChronicleProfiles.MEDIA_LOCAL_PROFILE } )
    public ByteBlobDataManager localBlobDataManager() {
        return new LocalBlobDataService( hikariDataSource );
    }

    @Bean( name = "byteBlobDataManager" )
    @DependsOn( "chronicleConfiguration" )
    @Profile( { ChronicleProfiles.MEDIA_LOCAL_AWS_PROFILE, Profiles.AWS_CONFIGURATION_PROFILE, Profiles.AWS_TESTING_PROFILE } )
    public ByteBlobDataManager awsByteBlobDataManager() {
        return new AwsBlobDataService( chronicleConfiguration(), executorService );
    }
}
