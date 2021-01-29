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
import com.openlattice.chronicle.constants.*;
import com.openlattice.chronicle.services.download.ParticipantDataIterable;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.converter.AbstractGenericHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;

public class IterableCsvHttpMessageConverter
        extends AbstractGenericHttpMessageConverter<ParticipantDataIterable> {

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
    public ParticipantDataIterable read( Type type, Class<?> contextClass, HttpInputMessage inputMessage )
            throws HttpMessageNotReadableException {
        throw new UnsupportedOperationException( "CSV is not a supported input format" );
    }

    @Override
    protected void writeInternal( ParticipantDataIterable t, Type type, HttpOutputMessage outputMessage )
            throws IOException, HttpMessageNotWritableException {
        csvMapper.writer( getSchema( t ) ).writeValues( outputMessage.getBody() ).writeAll( t );
    }

    @Override
    protected boolean supports( Class<?> clazz ) {
        return Iterable.class.isAssignableFrom( clazz );
    }

    @Override
    protected ParticipantDataIterable readInternal(
            Class<? extends ParticipantDataIterable> clazz,
            HttpInputMessage inputMessage ) throws HttpMessageNotReadableException {
        throw new UnsupportedOperationException( "CSV is not a supported input format" );
    }

    private CsvSchema getSchema( ParticipantDataIterable iterable ) {
        Builder schemaBuilder = CsvSchema.builder();
        List<String> columns = iterable.getColumnTitles();

        columns.forEach( col -> schemaBuilder.addColumn( col, ColumnType.ARRAY ) );

        return schemaBuilder.setUseHeader( true ).build();
    }
}
