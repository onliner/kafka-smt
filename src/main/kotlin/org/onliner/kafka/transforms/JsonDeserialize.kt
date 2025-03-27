package org.onliner.kafka.transforms

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.common.cache.SynchronizedCache
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements
import org.apache.kafka.connect.transforms.util.SchemaUtil
import org.apache.kafka.connect.transforms.util.SimpleConfig

@Suppress("TooManyFunctions")
abstract class JsonDeserialize<R : ConnectRecord<R>?> : Transformation<R> {
    companion object {
        const val OVERVIEW_DOC = "Deserialize specified fields to JSON structure"

        val CONFIG_DEF: ConfigDef = ConfigDef()
            .define(
                "fields",
                ConfigDef.Type.LIST,
                ConfigDef.Importance.HIGH,
                "List of fields to deserialize"
            )

        private val cache = SynchronizedCache(LRUCache<Schema, Schema>(16))
        private val mapper = ObjectMapper()

        private const val PURPOSE = "onliner-kafka-smt-json-decode"
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
        val converted = hashMapOf<String, JsonNode>()

        for (field in schema.fields()) {
            if (_fields.contains(field.name())) {
                converted[field.name()] = mapper.readTree(value.getString(field.name()))
            }
        }

        val outputSchema = copySchema(schema, converted)
        val outputValue = Struct(outputSchema)

        for (field in outputSchema.fields()) {
            val name = field.name()

            if (converted.containsKey(name)) {
                outputValue.put(name, asConnectValue(converted[name]!!, field.schema()))
            } else {
                outputValue.put(name, value.get(name))
            }
        }

        return newRecord(record, outputSchema, outputValue)
    }

    private fun convert(value: Any?): Any? {
        if (value is String) {
            return mapper.readTree(value)
        }

        return value;
    }

    private fun copySchema(original: Schema, converted: HashMap<String, JsonNode>): Schema {
        val cached = cache.get(original)

        if (cached != null) {
            return cached
        }

        val output = SchemaUtil.copySchemaBasics(original)

        for (field in original.fields()) {
            var schema = field.schema()

            if (converted.containsKey(field.name())) {
                schema = asConnectSchema(converted[field.name()]!!) ?: continue
            }

            output.field(field.name(), schema)
        }

        cache.put(original, output)

        return output
    }

    private fun asConnectValue(value: JsonNode, schema: Schema): Any?
    {
        return when (schema.type()) {
            Schema.Type.BOOLEAN -> value.booleanValue()
            Schema.Type.INT16 -> value.shortValue()
            Schema.Type.INT32 -> value.intValue()
            Schema.Type.INT64 -> value.longValue()
            Schema.Type.FLOAT32 -> value.floatValue()
            Schema.Type.FLOAT64 -> value.doubleValue()
            Schema.Type.STRING, Schema.Type.BYTES -> value.textValue()
            Schema.Type.STRUCT -> {
                val struct = Struct(schema)

                for (field in schema.fields()) {
                    struct.put(field, asConnectValue(value.get(field.name()), field.schema()))
                }

                struct
            }
            else -> throw DataException("Couldn't translate unsupported schema type $schema.")
        }
    }

    @Suppress("ComplexMethod")
    private fun asConnectSchema(value: JsonNode): Schema? {
        return when {
            value.isBoolean -> Schema.BOOLEAN_SCHEMA
            value.isShort -> Schema.INT16_SCHEMA
            value.isInt -> Schema.INT32_SCHEMA
            value.isLong -> Schema.INT64_SCHEMA
            value.isFloat -> Schema.FLOAT32_SCHEMA
            value.isDouble -> Schema.FLOAT64_SCHEMA
            value.isBinary -> Schema.BYTES_SCHEMA
            value.isTextual -> Schema.STRING_SCHEMA
            value.isObject -> {
                val builder = SchemaBuilder.struct()

                for ((k,v) in value.fields()) {
                    val kSchema = asConnectSchema(v) ?: continue

                    builder.field(k, kSchema)
                }

                builder.build()
            }
            value.isArray -> throw DataException("JSON arrays unsupported")
            else -> null
        }
    }

    class Key<R : ConnectRecord<R>?> : JsonDeserialize<R>() {
        override fun operatingSchema(record: R?): Schema? = record?.keySchema()

        override fun operatingValue(record: R?): Any? = record?.key()

        override fun newRecord(record: R?, schema: Schema?, value: Any?): R = record!!.newRecord(
            record.topic(),
            record.kafkaPartition(),
            schema,
            value,
            record.valueSchema(),
            record.value(),
            record.timestamp()
        )
    }

    class Value<R : ConnectRecord<R>?> : JsonDeserialize<R>() {
        override fun operatingSchema(record: R?): Schema? = record?.valueSchema()

        override fun operatingValue(record: R?): Any? = record?.value()

        override fun newRecord(record: R?, schema: Schema?, value: Any?): R = record!!.newRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            schema,
            value,
            record.timestamp()
        )
    }
}
