package org.onliner.kafka.transforms

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.source.SourceRecord
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import java.util.*
import kotlin.test.Test
import kotlin.test.assertNull

internal class JsonDeserializeTest {
    private val xformKey: JsonDeserialize<SourceRecord> = JsonDeserialize.Key()
    private val xformValue: JsonDeserialize<SourceRecord> = JsonDeserialize.Value()

    @AfterEach
    fun teardown() {
        xformKey.close()
        xformValue.close()
    }

    @Test
    fun handlesNullValue() {
        configure(xformValue)
        val given = SourceRecord(
            null,
            null,
            "topic",
            0,
            null,
            null
        )
        val expected = null
        val actual: Any? = xformValue.apply(given).value()
        assertEquals(expected, actual)
    }

    @Test
    fun handlesNullKey() {
        configure(xformKey)
        val given = SourceRecord(
            null,
            null,
            "topic",
            0,
            null,
            null,
            null,
            null
        )
        val expected = null
        val actual: Any? = xformKey.apply(given).key()
        assertEquals(expected, actual)
    }

    @Test
    fun copyValueSchemaAndConvertFields() {
        configure(xformValue, "payload")

        data class Case(
            val label: String,
            val actual: String,
            val expected: Map<String, Any>,
        )

        val cases = listOf(
            Case(
                label = "Basic example",
                actual = """{"foo":"bar","baz":false}""",
                expected = mapOf(
                    "foo" to "bar",
                    "baz" to false
                ),
            ),
            Case(
                label = "Just to be sure",
                actual = """{"foo":"zed","baz":true}""",
                expected = mapOf(
                    "foo" to "zed",
                    "baz" to true
                ),
            ),
            Case(
                label = "Other field order",
                actual = """{"baz":true,"foo":""}""",
                expected = mapOf(
                    "foo" to "",
                    "baz" to true
                ),
            ),
            Case(
                label = "Introduce new field",
                actual = """{"foo":"bar","baz":false,"zed":1}""",
                expected = mapOf(
                    "foo" to "bar",
                    "baz" to false,
                    "zed" to 1
                ),
            ),
        )

        val inputSchema = SchemaBuilder
            .struct()
            .name("name")
            .version(1)
            .doc("doc")
            .field("payload", Schema.STRING_SCHEMA)
            .field("hello", Schema.STRING_SCHEMA)
            .build()

        for (c in cases) {
            val label = c.label
            val value = Struct(inputSchema)
                .put("payload", c.actual)
                .put("hello", "world")

            val original = SourceRecord(null, null, "test", 0, inputSchema, value)
            val transformed = xformValue.apply(original)

            val outputSchema = transformed.valueSchema()
            val outputValue = transformed.value() as Struct

            assertEquals(inputSchema.name(), outputSchema.name(), "Case $label")
            assertEquals(inputSchema.version(), outputSchema.version(), "Case $label")
            assertEquals(inputSchema.doc(), outputSchema.doc(), "Case $label")

            val payloadField = outputSchema.field("payload")
            val payloadSchema = payloadField.schema()
            val payloadStruct = outputValue.getStruct("payload")

            for ((field, expectedValue) in c.expected) {
                val fieldSchema = payloadSchema.field(field).schema()

                when (expectedValue) {
                    is String -> {
                        assertEquals(Schema.STRING_SCHEMA, fieldSchema, "Case $label: $field")
                        assertEquals(expectedValue, payloadStruct.getString(field), "Case $label: $field")
                    }
                    is Boolean -> {
                        assertEquals(Schema.BOOLEAN_SCHEMA, fieldSchema, "Case $label: $field")
                        assertEquals(expectedValue, payloadStruct.getBoolean(field), "Case $label: $field")
                    }
                    is Int -> {
                        assertEquals(Schema.INT32_SCHEMA, fieldSchema, "Case $label: $field")
                        assertEquals(expectedValue, payloadStruct.getInt32(field), "Case $label: $field")
                    }
                    else -> error("Unsupported type for field '$field'")
                }
            }

            assertEquals(Schema.STRING_SCHEMA, outputSchema.field("hello").schema(), "Case $label")
            assertEquals("world", outputValue.getString("hello"), "Case $label")
        }
    }

    @Test
    fun schemalessValueConvertField() {
        configure(xformValue, "payload")
        val original = mapOf(
            "int32" to 42,
            "payload" to "{\"foo\":\"bar\",\"baz\":false}"
        )

        val record = SourceRecord(null, null, "test", 0, null, original)
        val transformed = xformValue.apply(record).value() as Map<*, *>

        assertEquals(42, transformed["int32"])

        assertInstanceOf(ObjectNode::class.java, transformed["payload"])

        val payload = transformed["payload"] as ObjectNode

        assertEquals("bar", payload.get("foo").textValue())
        assertEquals(false, payload.get("baz").booleanValue())
    }

    @Test
    fun schemalessValueConvertNullField() {
        configure(xformValue, "payload")
        val original = mapOf(
            "int32" to 42,
            "payload" to null
        )

        val record = SourceRecord(null, null, "test", 0, null, original)
        val transformed = xformValue.apply(record).value() as Map<*, *>

        assertEquals(42, transformed["int32"])
        assertNull(transformed["payload"])
    }

    @Test
    fun passUnknownSchemaFields() {
        configure(xformValue, "unknown")
        val schema = SchemaBuilder
            .struct()
            .name("name")
            .version(1)
            .doc("doc")
            .field("int32", Schema.INT32_SCHEMA)
            .build()

        val expected = Struct(schema).put("int32", 42)
        val original = SourceRecord(null, null, "test", 0, schema, expected)
        val transformed: SourceRecord = xformValue.apply(original)

        assertEquals(schema.name(), transformed.valueSchema().name())
        assertEquals(schema.version(), transformed.valueSchema().version())
        assertEquals(schema.doc(), transformed.valueSchema().doc())
        assertEquals(Schema.INT32_SCHEMA, transformed.valueSchema().field("int32").schema())
        assertEquals(42, (transformed.value() as Struct).getInt32("int32"))
    }

    @Test
    fun topLevelStructRequired() {
        configure(xformValue)
        assertThrows(DataException::class.java) {
            xformValue.apply(
                SourceRecord(
                    null, null,
                    "topic", 0, Schema.INT32_SCHEMA, 42
                )
            )
        }
    }

    @Test
    fun topLevelMapRequired() {
        configure(xformValue)
        assertThrows(DataException::class.java) {
            xformValue.apply(
                SourceRecord(
                    null, null,
                    "topic", 0, null, 42
                )
            )
        }
    }

    @Test
    fun testOptionalStruct() {
        configure(xformValue)
        val builder = SchemaBuilder.struct().optional()
        builder.field("opt_int32", Schema.OPTIONAL_INT32_SCHEMA)
        val schema = builder.build()
        val transformed: SourceRecord = xformValue.apply(
            SourceRecord(
                null, null,
                "topic", 0,
                schema, null
            )
        )
        assertEquals(Schema.Type.STRUCT, transformed.valueSchema().type())
        assertNull(transformed.value())
    }

    private fun configure(transform: JsonDeserialize<SourceRecord>, fields: String = "") {
        val props: MutableMap<String, String> = HashMap()

        props["fields"] = fields

        transform.configure(props.toMap())
    }
}
