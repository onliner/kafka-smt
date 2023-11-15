package org.onliner.kafka.transforms

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
            "longitude" to 27.5666700,
        )

        val record = SourceRecord(null, null, "test", 0, null, original)
        val transformed = xformValue.apply(record).value() as Map<*, *>

        Assertions.assertEquals(1, transformed["id"])
        Assertions.assertEquals("53.9,27.56667", transformed["location"])
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
