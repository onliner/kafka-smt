package org.onliner.kafka.transforms

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import kotlin.test.Test

internal class ConcatFieldsTest {
    private val xformKey: ConcatFields<SourceRecord> = ConcatFields.Key()
    private val xformValue: ConcatFields<SourceRecord> = ConcatFields.Value()

    @AfterEach
    fun teardown() {
        xformKey.close()
        xformValue.close()
    }

    @Test
    fun schemalessValueConcatFields() {
        configure(xformValue, listOf("latitude", "longitude"), ",", "location")

        val original = mapOf(
            "id" to 1,
            "latitude" to 53.9000000,
            "longitude" to -27.5666700,
        )

        val record = SourceRecord(null, null, "test", 0, null, original)
        val transformed = xformValue.apply(record).value() as Map<*, *>

        Assertions.assertEquals(1, transformed["id"])
        Assertions.assertEquals(53.9000000, transformed["latitude"])
        Assertions.assertEquals(-27.5666700, transformed["longitude"])
        Assertions.assertEquals("53.9,-27.56667", transformed["location"])
    }

    @Test
    fun copyValueSchemaAndConvertFields() {
        configure(xformValue, listOf("latitude", "longitude"), ",", "location")

        val schema = SchemaBuilder
            .struct()
            .name("name")
            .version(1)
            .doc("doc")
            .field("id", Schema.INT32_SCHEMA)
            .field("latitude", Schema.FLOAT32_SCHEMA)
            .field("longitude", Schema.FLOAT32_SCHEMA)
            .build()

        val value = Struct(schema)
            .put("id", 1)
            .put("latitude", 53.9000000.toFloat())
            .put("longitude", (-27.5666700).toFloat())

        val original = SourceRecord(null, null, "test", 0, schema, value)
        val transformed: SourceRecord = xformValue.apply(original)

        Assertions.assertEquals(schema.name(), transformed.valueSchema().name())
        Assertions.assertEquals(schema.version(), transformed.valueSchema().version())
        Assertions.assertEquals(schema.doc(), transformed.valueSchema().doc())

        Assertions.assertEquals(Schema.STRING_SCHEMA, transformed.valueSchema().field("location").schema())
        Assertions.assertEquals("53.9,-27.56667", (transformed.value() as Struct).get("location"))

        Assertions.assertEquals(Schema.INT32_SCHEMA, transformed.valueSchema().field("id").schema())
        Assertions.assertEquals(1, (transformed.value() as Struct).getInt32("id"))

        Assertions.assertEquals(Schema.FLOAT32_SCHEMA, transformed.valueSchema().field("latitude").schema())
        Assertions.assertEquals(53.9000000.toFloat(), (transformed.value() as Struct).getFloat32("latitude"))

        Assertions.assertEquals(Schema.FLOAT32_SCHEMA, transformed.valueSchema().field("longitude").schema())
        Assertions.assertEquals((-27.5666700).toFloat(), (transformed.value() as Struct).getFloat32("longitude"))
    }

    private fun configure(
        transform: ConcatFields<SourceRecord>,
        fields: List<String>,
        delimiter: String,
        output: String
    ) {
        val props = mapOf("fields" to fields, "delimiter" to delimiter, "output" to output)

        transform.configure(props.toMap())
    }
}
