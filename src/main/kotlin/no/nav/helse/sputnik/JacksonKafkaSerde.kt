package no.nav.helse.sputnik

import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer

class JacksonKafkaSerializer : Serializer<JsonNode> {
    override fun serialize(topic: String?, data: JsonNode?): ByteArray = objectMapper.writeValueAsBytes(data)
}

class JacksonKafkaDeserializer: Deserializer<JsonNode> {
    override fun deserialize(topic: String?, data: ByteArray): JsonNode = objectMapper.readTree(data)
}
