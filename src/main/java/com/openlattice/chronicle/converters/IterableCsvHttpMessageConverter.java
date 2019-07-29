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

package com.openlattice.chronicle.converters;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.Builder;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.ColumnType;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.google.common.collect.Sets;
import com.openlattice.chronicle.constants.CustomMediaType;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.converter.AbstractGenericHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;

public class IterableCsvHttpMessageConverter
        extends AbstractGenericHttpMessageConverter<Iterable<Map<String, Set<?>>>> {

    private final CsvMapper csvMapper = new CsvMapper();

    public IterableCsvHttpMessageConverter() {
        super( CustomMediaType.TEXT_CSV );
        csvMapper.registerModule( new AfterburnerModule() );
        csvMapper.registerModule( new GuavaModule() );
        csvMapper.registerModule( new JodaModule() );
        csvMapper.registerModule( new JavaTimeModule() );
        csvMapper.registerModule( new Jdk8Module() );
        csvMapper.configure( SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false );
        csvMapper.configure( CsvGenerator.Feature.STRICT_CHECK_FOR_QUOTING, true );
    }

    @Override
    public Iterable<Map<String, Set<?>>> read( Type type, Class<?> contextClass, HttpInputMessage inputMessage )
            throws HttpMessageNotReadableException {
        throw new UnsupportedOperationException( "CSV is not a supported input format" );
    }

    @Override
    protected void writeInternal( Iterable<Map<String, Set<?>>> t, Type type, HttpOutputMessage outputMessage )
            throws IOException, HttpMessageNotWritableException {
        csvMapper.writer( getSchema( t ) ).writeValues( outputMessage.getBody() ).writeAll( t );
    }

    @Override
    protected boolean supports( Class<?> clazz ) {
        return Iterable.class.isAssignableFrom( clazz );
    }

    @Override
    protected Iterable<Map<String, Set<?>>> readInternal(
            Class<? extends Iterable<Map<String, Set<?>>>> clazz,
            HttpInputMessage inputMessage ) throws HttpMessageNotReadableException {
        throw new UnsupportedOperationException( "CSV is not a supported input format" );
    }

    private CsvSchema getSchema( Iterable<Map<String, Set<?>>> maps ) {
        Set<String> columns = Sets.newLinkedHashSet();
        maps.forEach( map -> columns.addAll( map.keySet() ) );
        Builder schemaBuilder = CsvSchema.builder();
        columns.forEach( column -> schemaBuilder.addColumn( column, ColumnType.ARRAY ) );
        if ( columns.size() > 0 ) {
            schemaBuilder.setUseHeader( true );
        }
        return schemaBuilder.build();
    }

}
