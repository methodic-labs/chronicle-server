

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

package com.openlattice.chronicle.hazelcast.pods;

import com.kryptnostic.rhizome.hazelcast.serializers.ListStreamSerializers.DelegatedUUIDListStreamSerializer;
import com.openlattice.chronicle.hazelcast.StreamSerializerTypeIds;
import com.openlattice.chronicle.serializers.SharedStreamSerializers;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.stereotype.Component;

@Configuration
@ComponentScan(
        basePackageClasses = { SharedStreamSerializers.class },
        includeFilters = @ComponentScan.Filter(
                value = { Component.class },
                type = FilterType.ANNOTATION ) )
public class SharedStreamSerializersPod {
    @Bean
    public DelegatedUUIDListStreamSerializer delegatedUUIDListStreamSerializer() {
        return new DelegatedUUIDListStreamSerializer() {
            @Override public int getTypeId() {
                return StreamSerializerTypeIds.DELEGATED_UUID_LIST.ordinal();
            }
        };
    }
}

