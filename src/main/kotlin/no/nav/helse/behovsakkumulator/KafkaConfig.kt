package no.nav.helse.behovsakkumulator

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler
import java.util.Properties



fun loadBaseConfig(env: Environment, serviceUser: ServiceUser): Properties = Properties().also {
    it.load(Environment::class.java.getResourceAsStream("/kafka_base.properties"))
    it["sasl.jaas.config"] = "org.apache.kafka.common.security.plain.PlainLoginModule required " +
            "username=\"${serviceUser.username}\" password=\"${serviceUser.password}\";"
    it["bootstrap.servers"] = env.kafkaBootstrapServers
}

fun Properties.toStreamsConfig(): Properties = Properties().also {
    it.putAll(this)
    it[StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG] = LogAndContinueExceptionHandler::class.java
    it[StreamsConfig.APPLICATION_ID_CONFIG] = "behovsakkumulator-v1"
}
