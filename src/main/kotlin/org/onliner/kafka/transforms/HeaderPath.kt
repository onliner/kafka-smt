package org.onliner.kafka.transforms

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.header.Headers
import org.apache.kafka.connect.transforms.field.FieldSyntaxVersion
import org.apache.kafka.connect.transforms.field.SingleFieldPath
import org.apache.kafka.connect.transforms.util.Requirements

class HeaderPath(path: String) {
    companion object {
        private const val PURPOSE = "onliner-kafka-header-path"

        private val mapper = ObjectMapper()
        private val typeRef = object : TypeReference<Map<String, Any>>() {}
    }

    private val topPath: String
    private val fieldPath: SingleFieldPath?

    init {
        val parts = path.split('.', limit = 2)

        topPath = parts[0]
        fieldPath = parts.getOrNull(1)?.let { SingleFieldPath(it, FieldSyntaxVersion.V2) }
    }

    fun valueFrom(headers: Headers, default: Any): Any {
        val rawValue = headers.lastWithName(topPath)?.value() ?: return default
        val value = when {
            fieldPath == null -> rawValue
            rawValue is Struct -> fieldPath.valueFrom(rawValue)
            rawValue is String -> readJson(rawValue)
            else -> fieldPath.valueFrom(Requirements.requireMapOrNull(rawValue, PURPOSE))
        }

        return value ?: default
    }

    private fun readJson(value: String): Any? {
        val map = mapper.readValue(value, typeRef)

        return fieldPath?.valueFrom(map)
    }
}
