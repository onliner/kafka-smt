package org.onliner.kafka.transforms

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.SimpleConfig

@Suppress("TooManyFunctions")
class GroupPartitioner<R : ConnectRecord<R>?> : Transformation<R> {
    companion object {
        const val OVERVIEW_DOC = "Provides simple way to archive uniform partition distribution"

        private const val KEY_PARTITIONS = "partitions"
        private const val KEY_WORKERS = "workers"
        private const val KEY_GROUP = "group_key"
        private const val KEY_SPREAD = "spread_key"

        val CONFIG_DEF: ConfigDef = ConfigDef()
            .define(KEY_PARTITIONS, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "Total number of topic partitions")
            .define(KEY_WORKERS, ConfigDef.Type.INT, 0, ConfigDef.Importance.MEDIUM, "Number of workers")
            .define(KEY_GROUP, ConfigDef.Type.STRING, "group", ConfigDef.Importance.MEDIUM, "Group key name")
            .define(KEY_SPREAD, ConfigDef.Type.STRING, "spread", ConfigDef.Importance.MEDIUM, "Spread key name")
    }

    private var partitions: Int = 0
    private var workers: Int = 0
    private var shift: Int = 0

    private lateinit var groupKey: HeaderPath
    private lateinit var spreadKey: HeaderPath

    private val groupCounters = mutableMapOf<String, Int>()

    override fun configure(props: Map<String?, *>?) {
        val config = SimpleConfig(CONFIG_DEF, props)

        partitions = config.getInt(KEY_PARTITIONS)
        workers = config.getInt(KEY_WORKERS).takeIf { it > 0 && it <= partitions } ?: partitions
        shift = partitions / workers

        groupKey = HeaderPath(config.getString(KEY_GROUP))
        spreadKey = HeaderPath(config.getString(KEY_SPREAD))
    }

    override fun apply(record: R): R {
        val partition = record?.kafkaPartition()
        if (partition != null && partition != 0) return record

        val headers = record!!.headers()
        val group = groupKey.valueFrom(headers, "").toString()
        val spread = spreadKey.valueFrom(headers, workers).toString().toInt().coerceAtMost(workers)

        val start = Utils.toPositive(group.hashCode()) % (partitions - 1)
        val current = groupCounters.getOrDefault(group, start)
        val max = (start + spread * shift).coerceAtMost(partitions)

        val assignedPartition = if (current >= max) current - spread * shift else current

        groupCounters[group] = assignedPartition + shift

        return record.newRecord(
            record.topic(),
            assignedPartition,
            record.keySchema(),
            record.key(),
            record.valueSchema(),
            record.value(),
            record.timestamp(),
            record.headers(),
        )
    }

    @Suppress("EmptyFunctionBlock")
    override fun close() {
    }

    override fun config(): ConfigDef = CONFIG_DEF
}
