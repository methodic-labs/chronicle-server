package com.openlattice.chronicle.pods;

import com.amazonaws.services.s3.AmazonS3;
import com.kryptnostic.rhizome.configuration.ConfigurationConstants.Profiles;
import com.kryptnostic.rhizome.configuration.amazon.AmazonLaunchConfiguration;
import com.kryptnostic.rhizome.configuration.service.ConfigurationService;
import com.openlattice.ResourceConfigurationLoader;
import com.openlattice.chronicle.configuration.ChronicleConfiguration;
import java.io.IOException;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Configuration
public class ChronicleConfigurationPod {
    private static Logger logger = LoggerFactory.getLogger( ChronicleConfigurationPod.class );
    @Autowired( required = false )
    private AmazonS3 s3;

    @Autowired( required = false )
    private AmazonLaunchConfiguration awsLaunchConfig;

    @Inject
    private ConfigurationService configurationService;

    @Bean( name = "conductorConfiguration" )
    @Profile( Profiles.LOCAL_CONFIGURATION_PROFILE )
    public ChronicleConfiguration getLocalConfiguration() throws IOException {
        ChronicleConfiguration config = configurationService.getConfiguration( ChronicleConfiguration.class );
        logger.info( "Using local configuration: {}", config );
        return config;
    }

    @Bean( name = "conductorConfiguration" )
    @Profile( { Profiles.AWS_CONFIGURATION_PROFILE, Profiles.AWS_TESTING_PROFILE } )
    public ChronicleConfiguration getAwsConfiguration() throws IOException {

        ChronicleConfiguration config = ResourceConfigurationLoader.loadConfigurationFromS3( s3,
                awsLaunchConfig.getBucket(),
                awsLaunchConfig.getFolder(),
                ChronicleConfiguration.class );

        logger.info( "Using aws configuration: {}", config );
        return config;
    }
}
