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
package com.openlattice.chronicle.converters

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.dataformat.csv.CsvGenerator
import com.fasterxml.jackson.dataformat.csv.CsvMapper
import com.fasterxml.jackson.dataformat.csv.CsvSchema
import com.fasterxml.jackson.datatype.guava.GuavaModule
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.blackbird.BlackbirdModule
import com.openlattice.chronicle.constants.CustomMediaType
import org.springframework.http.HttpInputMessage
import org.springframework.http.HttpOutputMessage
import org.springframework.http.converter.AbstractGenericHttpMessageConverter
import org.springframework.http.converter.HttpMessageNotReadableException
import org.springframework.http.converter.HttpMessageNotWritableException
import java.io.IOException
import java.lang.reflect.Type
import java.util.*
import java.util.function.Consumer

class LegacyPostgresDownloadCsvHttpMessageConverter : AbstractGenericHttpMessageConverter<LegacyPostgresDownloadWrapper>(
    CustomMediaType.TEXT_CSV
) {
    private val csvMapper = CsvMapper()

    init {
        csvMapper.registerModule(BlackbirdModule())
        csvMapper.registerModule(GuavaModule())
        csvMapper.registerModule(JodaModule())
        csvMapper.registerModule(JavaTimeModule())
        csvMapper.registerModule(Jdk8Module())
        csvMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
        csvMapper.configure(CsvGenerator.Feature.STRICT_CHECK_FOR_QUOTING, true)
    }

    @Throws(HttpMessageNotReadableException::class)
    override fun read(type: Type, contextClass: Class<*>, inputMessage: HttpInputMessage): LegacyPostgresDownloadWrapper {
        throw UnsupportedOperationException("CSV is not a supported input format")
    }

    @Throws(IOException::class, HttpMessageNotWritableException::class)
    override fun writeInternal(t: LegacyPostgresDownloadWrapper, type: Type, outputMessage: HttpOutputMessage) {
        csvMapper.writer(getSchema(t)).writeValues(outputMessage.body).writeAll(t)
    }

    override fun supports(clazz: Class<*>): Boolean {
        return LegacyPostgresDownloadWrapper::class.java.isAssignableFrom(clazz)
    }

    @Throws(HttpMessageNotReadableException::class)
    override fun readInternal(
        clazz: Class<out LegacyPostgresDownloadWrapper>,
        inputMessage: HttpInputMessage
    ): LegacyPostgresDownloadWrapper {
        throw UnsupportedOperationException("CSV is not a supported input format")
    }

    private fun getSchema(iterable: LegacyPostgresDownloadWrapper): CsvSchema {

        val schemaBuilder = CsvSchema.builder()
        val columns: List<String> = iterable.columnAdvice
        columns.forEach(Consumer { col: String ->
            schemaBuilder.addColumn(col, CsvSchema.ColumnType.ARRAY)
        })

        return schemaBuilder.setUseHeader(true).build()
    }
}