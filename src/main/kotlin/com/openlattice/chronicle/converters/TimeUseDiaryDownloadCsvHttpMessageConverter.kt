package com.openlattice.chronicle.converters

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.dataformat.csv.CsvGenerator
import com.fasterxml.jackson.dataformat.csv.CsvMapper
import com.fasterxml.jackson.dataformat.csv.CsvSchema
import com.fasterxml.jackson.datatype.guava.GuavaModule
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.afterburner.AfterburnerModule
import com.openlattice.chronicle.constants.CustomMediaType
import org.springframework.http.HttpInputMessage
import org.springframework.http.HttpOutputMessage
import org.springframework.http.converter.AbstractGenericHttpMessageConverter
import java.lang.reflect.Type

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
class TimeUseDiaryDownloadCsvHttpMessageConverter : AbstractGenericHttpMessageConverter<TimeUseDiaryPostgresDownloadWrapper>(
    CustomMediaType.TEXT_CSV
) {
    private val csvMapper = CsvMapper()

    init {
        csvMapper.registerModule(AfterburnerModule())
        csvMapper.registerModule(GuavaModule())
        csvMapper.registerModule(JodaModule())
        csvMapper.registerModule(JavaTimeModule())
        csvMapper.registerModule(Jdk8Module())
        csvMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
        csvMapper.configure(CsvGenerator.Feature.STRICT_CHECK_FOR_QUOTING, true)
    }

    override fun read(type: Type, contextClass: Class<*>?, inputMessage: HttpInputMessage): TimeUseDiaryPostgresDownloadWrapper {
        throw UnsupportedOperationException("CSV is not a a supported input format")
    }

    override fun readInternal(clazz: Class<out TimeUseDiaryPostgresDownloadWrapper>, inputMessage: HttpInputMessage): TimeUseDiaryPostgresDownloadWrapper {
        throw UnsupportedOperationException("CSV is not a a supported input format")
    }

    override fun supports(clazz: Class<*>): Boolean {
        return TimeUseDiaryPostgresDownloadWrapper::class.java.isAssignableFrom(clazz)
    }

    override fun writeInternal(t: TimeUseDiaryPostgresDownloadWrapper, type: Type?, outputMessage: HttpOutputMessage) {
       csvMapper.writer(getSchema(t)).writeValues(outputMessage.body).writeAll(t)
    }

    private fun getSchema(iterable: TimeUseDiaryPostgresDownloadWrapper): CsvSchema {
        val schemaBuilder = CsvSchema.builder()
        val columns: List<String> = iterable.columnAdvice
        columns.forEach {
            schemaBuilder.addColumn(it)
        }

        return schemaBuilder.setUseHeader(true).build()
    }
}
