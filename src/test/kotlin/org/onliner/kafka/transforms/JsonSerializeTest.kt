package org.onliner.kafka.transforms

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

internal class JsonSerializeTest {
    private val xformKey: JsonSerialize<SourceRecord> = JsonSerialize.Key()
    private val xformValue: JsonSerialize<SourceRecord> = JsonSerialize.Value()

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

        val innerSchema = SchemaBuilder
            .struct()
            .name("payload")
            .version(1)
            .field("foo", Schema.STRING_SCHEMA)
            .field("bar", Schema.BOOLEAN_SCHEMA)

        val schema = SchemaBuilder
            .struct()
            .name("name")
            .version(1)
            .doc("doc")
            .field("payload", innerSchema)
            .field("string", Schema.STRING_SCHEMA)
            .build()

        val value = Struct(schema)
            .put("payload", Struct(innerSchema)
                .put("foo", "baz")
                .put("bar", false)
            )
            .put("string", "string")

        val original = SourceRecord(null, null, "test", 0, schema, value)
        val transformed: SourceRecord = xformValue.apply(original)

        assertEquals(schema.name(), transformed.valueSchema().name())
        assertEquals(schema.version(), transformed.valueSchema().version())
        assertEquals(schema.doc(), transformed.valueSchema().doc())

        assertEquals(Schema.STRING_SCHEMA, transformed.valueSchema().field("payload").schema())
        assertEquals("{\"bar\":false,\"foo\":\"baz\"}", (transformed.value() as Struct).get("payload"))

        assertEquals(Schema.STRING_SCHEMA, transformed.valueSchema().field("string").schema())
        assertEquals("string", (transformed.value() as Struct).getString("string"))
    }

    @Test
    fun schemalessValueConvertField() {
        configure(xformValue, "payload")
        val original = mapOf(
            "int32" to 42,
            "payload" to mapOf(
                "foo" to "bar",
                "baz" to false
            )
        )

        val record = SourceRecord(null, null, "test", 0, null, original)
        val transformed = xformValue.apply(record).value() as Map<*, *>

        assertEquals(42, transformed["int32"])
        assertEquals("{\"foo\":\"bar\",\"baz\":false}", transformed["payload"])
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

    private fun configure(transform: JsonSerialize<SourceRecord>, fields: String = "") {
        val props: MutableMap<String, String> = HashMap()

        props["fields"] = fields

        transform.configure(props.toMap())
    }
}
