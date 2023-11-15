package org.onliner.kafka.transforms

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.common.cache.SynchronizedCache
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements
import org.apache.kafka.connect.transforms.util.SchemaUtil
import org.apache.kafka.connect.transforms.util.SimpleConfig

abstract class ConcatFields<R : ConnectRecord<R>?> : Transformation<R> {
    companion object {
        const val OVERVIEW_DOC = "Concat fields in one specified field with delimeter"

        val CONFIG_DEF: ConfigDef = ConfigDef()
            .define(
                "fields",
                ConfigDef.Type.LIST,
                ConfigDef.Importance.HIGH,
                "List of fields to concat"
            )
            .define(
                "delimiter",
                ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH,
                "Delimiter for concat"
            )
            .define(
                "output",
                ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH,
                "Output field"
            )

        private val cache = SynchronizedCache(LRUCache<Schema, Schema>(16))

        private const val PURPOSE = "onliner-kafka-smt-concat-fields"
    }

    private lateinit var _fields: List<String>
    private lateinit var _delimiter: String
    private lateinit var _outputField: String

    override fun configure(props: Map<String?, *>?) {
        val config = SimpleConfig(CONFIG_DEF, props)

        _fields = config.getList("fields")
        _delimiter = config.getString("delimiter")
        _outputField = config.getString("output")
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
        val output = mutableListOf<String>()

        for (field in _fields) {
            if (value.containsKey(field)) {
                output.add(value[field].toString())
            }
        }

        value[_outputField] = output.joinToString(separator = _delimiter)

        return newRecord(record, null, value)
    }

    private fun applyWithSchema(record: R): R {
        val value = Requirements.requireStruct(operatingValue(record), PURPOSE)
        val schema = operatingSchema(record) ?: return record

        val outputSchema = copySchema(schema)
        val output = mutableListOf<String>()

        for (field in schema.fields()) {
            if (_fields.contains(field.name())) {
                output.add(value.getStruct(field.name()).toString())
            }
        }

        value.put(_outputField, output.joinToString(separator = _delimiter))

        return newRecord(record, outputSchema, value)
    }

    private fun copySchema(schema: Schema): Schema {
        val cached = cache.get(schema)

        if (cached != null) {
            return cached
        }

        val output = SchemaUtil.copySchemaBasics(schema)

        schema.fields().forEach { field -> output.field(field.name(), field.schema()) }
        output.field(_outputField, Schema.STRING_SCHEMA)

        cache.put(schema, output)

        return output
    }

    class Key<R : ConnectRecord<R>?> : ConcatFields<R>() {
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

    class Value<R : ConnectRecord<R>?> : ConcatFields<R>() {
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
