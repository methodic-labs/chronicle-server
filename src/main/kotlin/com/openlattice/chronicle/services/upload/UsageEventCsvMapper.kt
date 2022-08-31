package com.openlattice.chronicle.services.upload

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.dataformat.csv.CsvGenerator
import com.fasterxml.jackson.dataformat.csv.CsvMapper
import com.fasterxml.jackson.dataformat.csv.CsvSchema
import com.fasterxml.jackson.datatype.guava.GuavaModule
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.afterburner.AfterburnerModule
import com.geekbeast.postgres.PostgresDatatype
import com.openlattice.chronicle.storage.RedshiftDataTables.Companion.CHRONICLE_USAGE_EVENTS
import java.io.StringWriter
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@getmethodic.com&gt;
 */
class UsageEventCsvMapper {
    companion object {
        private val csvTypes = EnumMap<PostgresDatatype, CsvSchema.ColumnType>(PostgresDatatype::class.java)

        init {
            csvTypes[PostgresDatatype.TEXT] = CsvSchema.ColumnType.STRING
            csvTypes[PostgresDatatype.TEXT_128] = CsvSchema.ColumnType.STRING
            csvTypes[PostgresDatatype.TEXT_256] = CsvSchema.ColumnType.STRING
            csvTypes[PostgresDatatype.TEXT_UUID] = CsvSchema.ColumnType.STRING
            csvTypes[PostgresDatatype.TEXT_ARRAY] = CsvSchema.ColumnType.ARRAY
            csvTypes[PostgresDatatype.JSONB] = CsvSchema.ColumnType.STRING
            csvTypes[PostgresDatatype.UUID] = CsvSchema.ColumnType.STRING
            csvTypes[PostgresDatatype.UUID_ARRAY] = CsvSchema.ColumnType.ARRAY
            csvTypes[PostgresDatatype.TIMESTAMPTZ] = CsvSchema.ColumnType.STRING
            csvTypes[PostgresDatatype.BIGINT] = CsvSchema.ColumnType.NUMBER
            csvTypes[PostgresDatatype.DOUBLE] = CsvSchema.ColumnType.NUMBER
            csvTypes[PostgresDatatype.BOOLEAN] = CsvSchema.ColumnType.BOOLEAN
            csvTypes[PostgresDatatype.DATE] = CsvSchema.ColumnType.STRING
            csvTypes[PostgresDatatype.INTEGER] = CsvSchema.ColumnType.NUMBER
        }

        private val builder = CsvSchema.builder()
        private val schema : CsvSchema
        val csvMapper = CsvMapper()

        init {
            CHRONICLE_USAGE_EVENTS.columns.forEach { builder.addColumn(it.name, csvTypes[it.datatype]) }
            schema = builder.build().withHeader()

            csvMapper.registerModule(AfterburnerModule())
            csvMapper.registerModule(GuavaModule())
            csvMapper.registerModule(JodaModule())
            csvMapper.registerModule(JavaTimeModule())
            csvMapper.registerModule(Jdk8Module())
            csvMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
            csvMapper.configure(CsvGenerator.Feature.STRICT_CHECK_FOR_QUOTING, true)
        }

        fun writeValueAsString( event: UsageEventRow ) : String {
            return csvMapper.writerFor(UsageEventRow::class.java).with(schema).writeValueAsString(event)
        }

        fun writeList(events: List<UsageEventRow>) : String {
            val sw = StringWriter()
            sw.write("")

            return csvMapper.writerFor(object: TypeReference<List<UsageEventRow>>(){}).with(schema).writeValueAsString(events)
        }
    }
}