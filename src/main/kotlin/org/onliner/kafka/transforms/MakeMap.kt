package org.onliner.kafka.transforms

import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.common.cache.SynchronizedCache
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements
import org.apache.kafka.connect.transforms.util.SchemaUtil
import org.apache.kafka.connect.transforms.util.SimpleConfig

@Suppress("TooManyFunctions")
abstract class MakeMap<R : ConnectRecord<R>?> : Transformation<R> {
    companion object {
        const val OVERVIEW_DOC = "Put several fields to the Map structure"

        val CONFIG_DEF: ConfigDef = ConfigDef()
            .define(
                "fields",
                ConfigDef.Type.LIST,
                ConfigDef.Importance.HIGH,
                "List of fields to deserialize"
            )
            .define(
                "output",
                ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH,
                "Output field"
            )

        private val cache = SynchronizedCache(LRUCache<Schema, Schema>(16))

        private const val PURPOSE = "onliner-kafka-smt-make-map"
    }

    private lateinit var _fields: Map<String,String>
    private lateinit var _outputField: String

    override fun configure(props: Map<String?, *>?) {
        val config = SimpleConfig(CONFIG_DEF, props)

        _fields = parseMappings(config.getList("fields"))
        _outputField = config.getString("output")
    }

    private fun parseMappings(mappings: List<String>): Map<String, String> =
        mappings.associate { mapping ->
            val parts = mapping.split(":")

            if (parts.size != 2) {
                throw IllegalArgumentException("Invalid fields mapping: $mapping")
            }

            parts[0] to parts[1]
        }

    override fun apply(record: R): R = when {
        operatingValue(record) == null -> {
            record
        }

        operatingSchema(record) == null -> {
            applySchemaless(record)
        }

        else -> {
            applyWithSchema(record)
        }
    }

    @Suppress("EmptyFunctionBlock")
    override fun close() {
    }

    override fun config(): ConfigDef = CONFIG_DEF

    protected abstract fun operatingSchema(record: R?): Schema?
    protected abstract fun operatingValue(record: R?): Any?
    protected abstract fun newRecord(record: R?, schema: Schema?, value: Any?): R

    private fun applySchemaless(record: R): R {
        val value = Requirements.requireMap(operatingValue(record), PURPOSE)
        val map = mutableMapOf<String, Any?>()

        for (field in _fields) {
            if (!value.containsKey(field.key)) {
                continue
            }

            map.put(field.value, value[field.key])
        }

        value[_outputField] = map

        return newRecord(record, null, value)
    }

    private fun applyWithSchema(record: R): R {
        val value = Requirements.requireStruct(operatingValue(record), PURPOSE)
        val schema = operatingSchema(record) ?: return record
        val map = Struct(mapSchema(schema))

        for (field in _fields) {
            map.put(field.value, value[field.key])
        }

        val outputSchema = copySchema(schema)
        val outputValue = copyValue(schema, outputSchema, value)

        outputValue.put(_outputField, map)

        return newRecord(record, outputSchema, outputValue)
    }

    private fun copySchema(schema: Schema): Schema {
        val cached = cache.get(schema)

        if (cached != null) {
            return cached
        }

        val output = SchemaUtil.copySchemaBasics(schema)

        schema.fields().forEach { field -> output.field(field.name(), field.schema()) }

        output.field(_outputField, mapSchema(schema))

        cache.put(schema, output)

        return output
    }

    private fun mapSchema(schema: Schema): Schema {
        val builder = SchemaBuilder.struct()

        _fields.forEach { (from, to) ->
            builder.field(to, schema.field(from).schema())
        }

        return builder.build()
    }

    private fun copyValue(oldSchema: Schema, newSchema: Schema, oldValue: Struct): Struct {
        val newValue = Struct(newSchema)

        oldSchema.fields().forEach { field -> newValue.put(field.name(), oldValue.get(field)) }

        return newValue
    }

    class Key<R : ConnectRecord<R>?> : MakeMap<R>() {
        override fun operatingSchema(record: R?): Schema? = record?.keySchema()

        override fun operatingValue(record: R?): Any? = record?.key()

        override fun newRecord(record: R?, schema: Schema?, value: Any?): R = record!!.newRecord(
            record.topic(),
            record.kafkaPartition(),
            schema,
            value,
            record.valueSchema(),
            record.value(),
            record.timestamp(),
            record.headers(),
        )
    }

    class Value<R : ConnectRecord<R>?> : MakeMap<R>() {
        override fun operatingSchema(record: R?): Schema? = record?.valueSchema()

        override fun operatingValue(record: R?): Any? = record?.value()

        override fun newRecord(record: R?, schema: Schema?, value: Any?): R = record!!.newRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            schema,
            value,
            record.timestamp(),
            record.headers(),
        )
    }
}
