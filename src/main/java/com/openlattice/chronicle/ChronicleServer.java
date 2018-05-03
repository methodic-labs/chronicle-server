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

package com.openlattice.chronicle;

import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.kryptnostic.rhizome.configuration.websockets.BaseRhizomeServer;
import com.kryptnostic.rhizome.core.RhizomeApplicationServer;
import com.kryptnostic.rhizome.hazelcast.serializers.RhizomeUtils.Pods;
import com.kryptnostic.rhizome.pods.hazelcast.RegistryBasedHazelcastInstanceConfigurationPod;
import com.openlattice.auth0.Auth0Pod;
import com.openlattice.aws.AwsS3Pod;
import com.openlattice.chronicle.pods.ChronicleServerSecurityPod;
import com.openlattice.chronicle.pods.ChronicleServerServicesPod;
import com.openlattice.chronicle.pods.ChronicleServerServletsPod;
import com.openlattice.data.serializers.FullQualifiedNameJacksonDeserializer;
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer;

public class ChronicleServer extends BaseRhizomeServer {
    public static final Class<?>[] webPods     = new Class<?>[] {
            ChronicleServerServletsPod.class,
            ChronicleServerSecurityPod.class, };
    public static final Class<?>[] rhizomePods = new Class<?>[] {
            RegistryBasedHazelcastInstanceConfigurationPod.class,
            Auth0Pod.class };

    public static final Class<?>[] chronicleServerPods = new Class<?>[] {
            AwsS3Pod.class,
            ChronicleServerServicesPod.class
    };

    static {
        ObjectMappers.foreach( FullQualifiedNameJacksonSerializer::registerWithMapper );
        ObjectMappers.foreach( FullQualifiedNameJacksonDeserializer::registerWithMapper );
        ObjectMappers.foreach( mapper -> mapper.disable( SerializationFeature.WRITE_DATES_AS_TIMESTAMPS ) );
    }

    public ChronicleServer( Class<?>... pods ) {
        super( Pods.concatenate(
                pods,
                webPods,
                rhizomePods,
                RhizomeApplicationServer.DEFAULT_PODS,
                chronicleServerPods ) );
    }

    @Override public void start( String... profiles ) throws Exception {
        super.start( profiles );
    }

    public static void main( String[] args ) throws Exception {
        ChronicleServer chronicleServer = new ChronicleServer();
        chronicleServer.start( args );
    }
}
