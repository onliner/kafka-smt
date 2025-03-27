package org.onliner.kafka.transforms

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import kotlin.test.Test

internal class InsertUuidTest {
    private val xformKey: InsertUuid<SourceRecord> = InsertUuid.Key()
    private val xformValue: InsertUuid<SourceRecord> = InsertUuid.Value()

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
        Assertions.assertEquals(expected, actual)
    }

    @Test
    fun schemalessValueConcatFields() {
        configure(xformValue)

        val original = mapOf(
            "foo" to 42,
            "bar" to "baz",
        )

        val record = SourceRecord(null, null, "test", 0, null, original)
        val transformed = xformValue.apply(record).value() as Map<*, *>

        Assertions.assertEquals(42, transformed["foo"])
        Assertions.assertEquals("baz", transformed["bar"])
        Assertions.assertEquals("65ac994f-52ee-52d3-b594-96f467899361", transformed["id"])
    }

    @Test
    fun copyValueSchemaAndConvertFields() {
        configure(xformValue)

        val schema = SchemaBuilder
            .struct()
            .name("name")
            .version(1)
            .doc("doc")
            .field("foo", Schema.INT32_SCHEMA)
            .field("bar", Schema.STRING_SCHEMA)
            .build()

        val value = Struct(schema)
            .put("foo", 42)
            .put("bar", "baz")

        val original = SourceRecord(null, null, "test", 0, schema, value)
        val transformed: SourceRecord = xformValue.apply(original)

        Assertions.assertEquals(schema.name(), transformed.valueSchema().name())
        Assertions.assertEquals(schema.version(), transformed.valueSchema().version())
        Assertions.assertEquals(schema.doc(), transformed.valueSchema().doc())

        Assertions.assertEquals(Schema.INT32_SCHEMA, transformed.valueSchema().field("foo").schema())
        Assertions.assertEquals(42, (transformed.value() as Struct).getInt32("foo"))

        Assertions.assertEquals(Schema.STRING_SCHEMA, transformed.valueSchema().field("bar").schema())
        Assertions.assertEquals("baz", (transformed.value() as Struct).get("bar"))

        Assertions.assertEquals(Schema.STRING_SCHEMA, transformed.valueSchema().field("id").schema())
        Assertions.assertEquals("65ac994f-52ee-52d3-b594-96f467899361", (transformed.value() as Struct).get("id"))
    }

    private fun configure(transform: InsertUuid<SourceRecord>) {
        val props = mapOf(
            "fields" to listOf("foo", "bar"),
            "output" to "id"
        )

        transform.configure(props.toMap())
    }
}
