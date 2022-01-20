package com.openlattice.chronicle.pods;

import com.amazonaws.services.s3.AmazonS3;
import com.geekbeast.ResourceConfigurationLoader;
import com.geekbeast.rhizome.configuration.ConfigurationConstants;
import com.geekbeast.rhizome.configuration.configuration.amazon.AmazonLaunchConfiguration;
import com.geekbeast.rhizome.configuration.service.ConfigurationService;
import com.openlattice.chronicle.configuration.ChronicleConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import javax.inject.Inject;
import java.io.IOException;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Configuration
public class ChronicleConfigurationPod {
    private static Logger   logger = LoggerFactory.getLogger( ChronicleConfigurationPod.class );
    @Autowired( required = false )
    private        AmazonS3 s3;

    @Autowired( required = false )
    private AmazonLaunchConfiguration awsLaunchConfig;

    @Inject
    private ConfigurationService configurationService;

    @Bean( name = "conductorConfiguration" )
    @Profile( ConfigurationConstants.Profiles.LOCAL_CONFIGURATION_PROFILE )
    public ChronicleConfiguration getLocalConfiguration() throws IOException {
        ChronicleConfiguration config = configurationService.getConfiguration( ChronicleConfiguration.class );
        logger.info( "Using local configuration: {}", config );
        return config;
    }

    @Bean( name = "conductorConfiguration" )
    @Profile( { ConfigurationConstants.Profiles.AWS_CONFIGURATION_PROFILE,
            ConfigurationConstants.Profiles.AWS_TESTING_PROFILE } )
    public ChronicleConfiguration getAwsConfiguration() throws IOException {

        ChronicleConfiguration config = ResourceConfigurationLoader.loadConfigurationFromS3( s3,
                awsLaunchConfig.getBucket(),
                awsLaunchConfig.getFolder(),
                ChronicleConfiguration.class );

        logger.info( "Using aws configuration: {}", config );
        return config;
    }
}
