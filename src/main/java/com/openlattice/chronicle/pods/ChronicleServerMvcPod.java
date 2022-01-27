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
 */

package com.openlattice.chronicle.pods;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.openlattice.chronicle.constants.CustomMediaType;
import com.openlattice.chronicle.controllers.StudyController;
import com.openlattice.chronicle.controllers.TimeUseDiaryController;
import com.openlattice.chronicle.controllers.legacy.ChronicleController;
import com.openlattice.chronicle.controllers.v2.ChronicleControllerV2;
import com.openlattice.chronicle.converters.IterableCsvHttpMessageConverter;
import com.openlattice.chronicle.converters.YamlHttpMessageConverter;
import com.openlattice.chronicle.controllers.ChronicleServerExceptionHandler;
import com.ryantenney.metrics.spring.config.annotation.EnableMetrics;
import java.util.List;
import javax.inject.Inject;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.web.servlet.config.annotation.ContentNegotiationConfigurer;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurationSupport;

@Configuration
@ComponentScan(
        basePackageClasses = {
                ChronicleController.class,
                ChronicleServerExceptionHandler.class,
                ChronicleControllerV2.class,
                StudyController.class,
                TimeUseDiaryController.class
        },
        includeFilters = @ComponentScan.Filter(
                value = {
                        org.springframework.stereotype.Controller.class,
                        org.springframework.web.bind.annotation.RestControllerAdvice.class
                },
                type = FilterType.ANNOTATION
        )
)
@EnableAsync
@EnableMetrics(
        proxyTargetClass = true )
public class ChronicleServerMvcPod extends WebMvcConfigurationSupport {
    public static final String FILE_TYPE = "fileType";
    @Inject
    private ObjectMapper defaultObjectMapper;

    @Inject
    private ChronicleServerSecurityPod chronicleServerSecurityPod;

    @Override
    protected void configureMessageConverters( List<HttpMessageConverter<?>> converters ) {
        super.addDefaultHttpMessageConverters( converters );
        for ( HttpMessageConverter<?> converter : converters ) {
            if ( converter instanceof MappingJackson2HttpMessageConverter ) {
                MappingJackson2HttpMessageConverter jacksonConverter = (MappingJackson2HttpMessageConverter) converter;
                jacksonConverter.setObjectMapper( defaultObjectMapper );
            }
        }
        converters.add( new IterableCsvHttpMessageConverter() );
        converters.add( new YamlHttpMessageConverter() );
    }

    // TODO: We need to lock this down. Since all endpoints are stateless + authenticated this is more a
    // defense-in-depth measure.
    @Override
    protected void addCorsMappings( CorsRegistry registry ) {
        registry
                .addMapping( "/**" )
                .allowedMethods( "GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH" )
                .allowedOrigins( "*" );
        super.addCorsMappings( registry );
    }

    @Override
    protected void configureContentNegotiation( ContentNegotiationConfigurer configurer ) {
        configurer.parameterName( FILE_TYPE )
                .favorParameter( true )
                .mediaType( "csv", CustomMediaType.TEXT_CSV )
                .mediaType( "json", MediaType.APPLICATION_JSON )
                .mediaType( "yaml", CustomMediaType.TEXT_YAML )
                .defaultContentType( MediaType.APPLICATION_JSON );
    }

    @Bean
    public AuthenticationManager authenticationManagerBean() throws Exception {
        return chronicleServerSecurityPod.authenticationManagerBean();
    }
}
