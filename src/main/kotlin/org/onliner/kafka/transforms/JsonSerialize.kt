package org.onliner.kafka.transforms

import com.fasterxml.jackson.databind.ObjectMapper
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
abstract class JsonSerialize<R : ConnectRecord<R>?> : Transformation<R> {
    companion object {
        const val OVERVIEW_DOC = "Serialize specified fields to JSON"

        val CONFIG_DEF: ConfigDef = ConfigDef()
            .define(
                "fields",
                ConfigDef.Type.LIST,
                ConfigDef.Importance.HIGH,
                "List of fields to serialize"
            )

        private val cache = SynchronizedCache(LRUCache<Schema, Schema>(16))
        private val mapper = ObjectMapper()

        private const val PURPOSE = "onliner-kafka-smt-json-encode"
    }

    private lateinit var _fields: List<String>

    override fun configure(props: Map<String?, *>?) {
        val config = SimpleConfig(CONFIG_DEF, props)

        _fields = config.getList("fields")
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

        for (field in _fields) {
            if (!value.containsKey(field)) {
                continue;
            }

            value[field] = convert(value[field])
        }

        return newRecord(record, null, value)
    }

    private fun applyWithSchema(record: R): R {
        val value = Requirements.requireStruct(operatingValue(record), PURPOSE)
        val schema = operatingSchema(record) ?: return record

        val outputSchema = copySchema(schema)
        val outputValue = Struct(outputSchema)

        for (field in schema.fields()) {
            if (_fields.contains(field.name())) {
                outputValue.put(field.name(), convert(value.getStruct(field.name())))
            } else {
                outputValue.put(field.name(), value.get(field))
            }
        }

        return newRecord(record, outputSchema, outputValue)
    }

    private fun convert(value: Any?): String? {
        if (value === null) {
            return null
        }

        return mapper.writeValueAsString(value);
    }

    private fun convert(value: Struct): Any? {
        return mapper.writeValueAsString(extractFields(value))
    }

    private fun extractFields(value: Struct): HashMap<String, Any?> {
        val hash = HashMap<String, Any?>()
        val fields = value.schema().fields()

        for (field in fields) {
            val name = field.name()
            val schema = field.schema()
            val type = schema.type()

            hash[name] = when {
                type === Schema.Type.STRUCT -> extractFields(value.getStruct(name))
                else -> value.get(field)
            }
        }

        return hash
    }

    private fun copySchema(schema: Schema): Schema {
        val cached = cache.get(schema)

        if (cached != null) {
            return cached
        }

        val output = SchemaUtil.copySchemaBasics(schema)

        for (field in schema.fields()) {
            val name = field.name()

            if (_fields.contains(name)) {
                output.field(name, SchemaBuilder.STRING_SCHEMA)
            } else {
                output.field(name, field.schema())
            }
        }

        cache.put(schema, output)

        return output
    }

    class Key<R : ConnectRecord<R>?> : JsonSerialize<R>() {
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

    class Value<R : ConnectRecord<R>?> : JsonSerialize<R>() {
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
