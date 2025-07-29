package org.onliner.kafka.transforms

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import kotlin.test.Test

internal class MakeMapTest {
    private val xformValue: MakeMap<SourceRecord> = MakeMap.Value()

    @AfterEach
    fun teardown() {
        xformValue.close()
    }

    @Test
    fun schemalessHandlesNullValue() {
        configure(xformValue)

        val given = SourceRecord(null, null, "topic", 0, null, null)
        val expected = null
        val actual: Any? = xformValue.apply(given).value()

        Assertions.assertEquals(expected, actual)
    }

    @Test
    fun schemalessHandlesPartlyMissedFields() {
        configure(xformValue)

        val original = mapOf(
            "id" to 1,
            "initiator_type" to "user",
        )

        val record = SourceRecord(null, null, "test", 0, null, original)
        val transformed = xformValue.apply(record).value() as Map<*, *>

        Assertions.assertEquals(1, transformed["id"])
        Assertions.assertEquals(mapOf("type" to "user"), transformed["initiator"])
    }

    @Test
    fun schemalessHandlesAllMissedFields() {
        configure(xformValue)

        val original = mapOf(
            "id" to 1,
            "value" to 2,
        )

        val record = SourceRecord(null, null, "test", 0, null, original)
        val transformed = xformValue.apply(record).value() as Map<*, *>

        Assertions.assertEquals(1, transformed["id"])
        Assertions.assertEquals(mapOf<String, Any?>(), transformed["initiator"])
    }

    @Test
    fun schemalessValueMakeMap() {
        configure(xformValue)

        val original = mapOf(
            "id" to 1,
            "initiator_type" to "user",
            "initiator_id" to "123",
        )

        val record = SourceRecord(null, null, "test", 0, null, original)
        val transformed = xformValue.apply(record).value() as Map<*, *>

        Assertions.assertEquals(1, transformed["id"])
        Assertions.assertEquals(mapOf("type" to "user", "id" to "123"), transformed["initiator"])
    }

    @Test
    fun schemaMakeMap() {
        configure(xformValue)

        val schema = SchemaBuilder
            .struct()
            .name("name")
            .version(1)
            .doc("doc")
            .field("id", Schema.INT32_SCHEMA)
            .field("initiator_type", Schema.STRING_SCHEMA)
            .field("initiator_id", Schema.STRING_SCHEMA)
            .build()

        val value = Struct(schema)
            .put("id", 1)
            .put("initiator_type", "user")
            .put("initiator_id", "123")

        val original = SourceRecord(null, null, "test", 0, schema, value)
        val transformed: SourceRecord = xformValue.apply(original)
        val transformedSchema = transformed.valueSchema()
        val outputStruct = (transformed.value() as Struct).getStruct("initiator")
        val outputSchema = transformedSchema.field("initiator").schema()

        Assertions.assertEquals(schema.name(), transformedSchema.name())
        Assertions.assertEquals(schema.version(), transformedSchema.version())
        Assertions.assertEquals(schema.doc(), transformedSchema.doc())

        Assertions.assertEquals(Schema.INT32_SCHEMA, transformedSchema.field("id").schema())
        Assertions.assertEquals(1, (transformed.value() as Struct).getInt32("id"))

        Assertions.assertEquals(Schema.STRING_SCHEMA, transformedSchema.field("initiator_type").schema())
        Assertions.assertEquals("user", (transformed.value() as Struct).getString("initiator_type"))

        Assertions.assertEquals(Schema.STRING_SCHEMA, transformedSchema.field("initiator_id").schema())
        Assertions.assertEquals("123", (transformed.value() as Struct).getString("initiator_id"))

        Assertions.assertEquals(Schema.STRING_SCHEMA, outputSchema.field("type").schema())
        Assertions.assertEquals("user", outputStruct.getString("type"))

        Assertions.assertEquals(Schema.STRING_SCHEMA, outputSchema.field("id").schema())
        Assertions.assertEquals("123", outputStruct.getString("id"))
    }

    @Test
    fun schemaHandlesOptionalFieldsMakeMap() {
        configure(xformValue)

        val schema = SchemaBuilder
            .struct()
            .name("name")
            .version(1)
            .doc("doc")
            .field("id", Schema.INT32_SCHEMA)
            .field("initiator_type", Schema.STRING_SCHEMA)
            .field("initiator_id", Schema.OPTIONAL_STRING_SCHEMA)
            .build()

        val value = Struct(schema)
            .put("id", 1)
            .put("initiator_type", "user")

        val original = SourceRecord(null, null, "test", 0, schema, value)
        val transformed: SourceRecord = xformValue.apply(original)
        val transformedSchema = transformed.valueSchema()
        val outputStruct = (transformed.value() as Struct).getStruct("initiator")
        val outputSchema = transformedSchema.field("initiator").schema()

        Assertions.assertEquals(Schema.INT32_SCHEMA, transformedSchema.field("id").schema())
        Assertions.assertEquals(1, (transformed.value() as Struct).getInt32("id"))

        Assertions.assertEquals(Schema.STRING_SCHEMA, outputSchema.field("type").schema())
        Assertions.assertEquals("user", outputStruct.getString("type"))

        Assertions.assertEquals(Schema.OPTIONAL_STRING_SCHEMA, outputSchema.field("id").schema())
        Assertions.assertEquals(null, outputStruct.getString("id"))
    }

    private fun configure(transform: MakeMap<SourceRecord>) {
        val props = mapOf("fields" to listOf("initiator_type:type", "initiator_id:id"), "output" to "initiator")

        transform.configure(props.toMap())
    }
}
