package org.onliner.kafka.transforms

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.connect.source.SourceRecord
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import java.util.*
import kotlin.test.Test

internal class GroupPartitionerTest {
    private val xTransform: GroupPartitioner<SourceRecord> = GroupPartitioner()

    @AfterEach
    fun teardown() {
        xTransform.close()
    }

    @Test
    fun handlesNulls() {
        configure()

        assertEquals(0, xTransform.apply(newRecord()).kafkaPartition())
        assertEquals(3, xTransform.apply(newRecord()).kafkaPartition())
        assertEquals(6, xTransform.apply(newRecord()).kafkaPartition())
        assertEquals(9, xTransform.apply(newRecord()).kafkaPartition())
        assertEquals(0, xTransform.apply(newRecord()).kafkaPartition())
        assertEquals(3, xTransform.apply(newRecord()).kafkaPartition())
    }

    @Test
    fun partitionsWithGroup() {
        configure()

        val spread = 3
        val record1 = newRecordInGroup("foo", spread)
        val record2 = newRecordInGroup("foo", spread)
        val record3 = newRecordInGroup("foo", spread)
        val record4 = newRecordInGroup("foo", spread)

        assertEquals(0, xTransform.apply(record1).kafkaPartition())
        assertEquals(3, xTransform.apply(record2).kafkaPartition())
        assertEquals(6, xTransform.apply(record3).kafkaPartition())
        assertEquals(0, xTransform.apply(record4).kafkaPartition())
    }

    @Test
    fun partitionsWithGroupInMetadata() {
        configure(
            groupKey = "metadata.group",
            spreadKey = "metadata.spread"
        )

        val spread = 3
        val record1 = newRecordInGroup("non", spread, true)
        val record2 = newRecordInGroup("non", spread, true)
        val record3 = newRecordInGroup("non", spread, true)
        val record4 = newRecordInGroup("non", spread, true)

        assertEquals(9, xTransform.apply(record1).kafkaPartition())
        assertEquals(3, xTransform.apply(record2).kafkaPartition())
        assertEquals(6, xTransform.apply(record3).kafkaPartition())
        assertEquals(9, xTransform.apply(record4).kafkaPartition())
    }

    @Test
    fun partitionsWithBigValues() {
        configure(
            partitions = 192,
            workers = 24,
            groupKey = "group",
            spreadKey = "spread"
        )

        val spread = 16
        val records = List(20) { newRecordInGroup("foo", spread) }
        val actual = records.map { xTransform.apply(it).kafkaPartition() }

        assertEquals(listOf(
            153, 161, 169, 177,
            185, 65, 73, 81,
            89, 97, 105, 113,
            121, 129, 137, 145,
            153, 161, 169, 177
        ), actual)
    }

    private fun configure(
        partitions: Int = 12,
        workers: Int = 4,
        groupKey: String = "group",
        spreadKey: String = "spread",
    ) {
        val props: MutableMap<String, String> = HashMap()

        props["partitions"] = partitions.toString()
        props["workers"] = workers.toString()
        props["group_key"] = groupKey
        props["spread_key"] = spreadKey

        xTransform.configure(props.toMap())
    }

    private fun newRecord(): SourceRecord {
        return SourceRecord(
            null,
            null,
            "topic",
            0,
            null,
            null,
        )
    }

    private fun newRecordInGroup(group: String, spread: Int = 0, metadata: Boolean = false): SourceRecord {
        val record = newRecord()

        if (metadata) {
            val mapper = ObjectMapper()

            record.headers().addString("metadata", mapper.writeValueAsString(mapOf(
                "group" to group,
                "spread" to spread,
            )))
        } else {
            record.headers().addString("group", group)
            record.headers().addInt("spread", spread)
        }

        return record
    }
}
