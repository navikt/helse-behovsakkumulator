package no.nav.helse.behovsakkumulator

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.treeToValue
import io.ktor.application.install
import io.ktor.metrics.micrometer.MicrometerMetrics
import io.ktor.routing.routing
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import net.logstash.logback.argument.StructuredArguments.keyValue
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Produced
import org.slf4j.LoggerFactory
import java.util.Properties
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

val meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
val objectMapper: ObjectMapper = jacksonObjectMapper()
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    .registerModule(JavaTimeModule())
val log = LoggerFactory.getLogger("behovsakkumulator")

fun main() = runBlocking {
    val serviceUser = readServiceUserCredentials()
    val environment = setUpEnvironment()

    launchApplication(environment, serviceUser)
}

fun launchApplication(
    environment: Environment,
    serviceUser: ServiceUser
) {
    val applicationContext = Executors.newFixedThreadPool(4).asCoroutineDispatcher()
    val exceptionHandler = CoroutineExceptionHandler { context, e ->
        log.error("Feil i lytter", e)
        context.cancel(CancellationException("Feil i lytter", e))
    }
    runBlocking(exceptionHandler + applicationContext) {
        val stream = createStream(environment, serviceUser)
        stream.start()

        val server = embeddedServer(Netty, 8080) {
            install(MicrometerMetrics) {
                registry = meterRegistry
            }

            routing {
                registerHealthApi(
                    liveness = { stream.state().isRunning },
                    readiness = { true },
                    meterRegistry = meterRegistry
                )
            }
        }.start(wait = false)

        Runtime.getRuntime().addShutdownHook(Thread {
            server.stop(10, 10, TimeUnit.SECONDS)
            stream.close()
            applicationContext.close()
        })
    }
}

fun createStream(
    environment: Environment,
    serviceUser: ServiceUser,
    baseConfig: Properties = loadBaseConfig(environment, serviceUser)
): KafkaStreams {
    val builder = StreamsBuilder()
    builder
        .stream<String, JsonNode>(
            environment.spleisBehovtopic,
            Consumed.with(Serdes.StringSerde(), JacksonKafkaSerde())
        )
        .alleBehovSomIkkeErMarkertFerdig()
        .kombinerDelløsningerPåBehov()
        .bareKomplettLøsningPåBehov()
        .markerBehovFerdig()
        .to(environment.spleisBehovtopic, Produced.with(Serdes.StringSerde(), JacksonKafkaSerde()))
    return KafkaStreams(builder.build(), baseConfig.toStreamsConfig()).apply {
        setUncaughtExceptionHandler { _, err ->
            log.error("Caught exception in stream: ${err.message}", err)
            close()
        }
    }
}

private fun KStream<String, JsonNode>.markerBehovFerdig(): KStream<String, JsonNode> =
    this.mapValues { value -> (value as ObjectNode).put("final", true) as JsonNode }
        .peek { key, _ -> log.info("Markert behov med {} som final", keyValue("id", key)) }

private fun KStream<String, JsonNode>.kombinerDelløsningerPåBehov(): KStream<String, JsonNode> =
    this.groupByKey()
        .reduce(::slåSammenLøsninger)
        .toStream()
        .peek { key, value ->
            log.info(
                "Satt sammen {} for behov med id {}. Forventer {}",
                keyValue("løsninger", value["@løsning"].fieldNames().asSequence().joinToString(", ")),
                keyValue("id", key),
                keyValue("behov", value["@behov"].asSequence().map(JsonNode::asText).joinToString(", "))
            )
        }


private fun KStream<String, JsonNode>.bareKomplettLøsningPåBehov(): KStream<String, JsonNode> =
    this.filter { _, value ->
        val løsninger = objectMapper.treeToValue<Map<String, JsonNode>>(value["@løsning"])
        val behov = objectMapper.treeToValue<List<String>>(value["@behov"])
        behov.all(løsninger::containsKey)
    }

private fun KStream<String, JsonNode>.alleBehovSomIkkeErMarkertFerdig(): KStream<String, JsonNode> =
    this.filter { _, value -> value.hasNonNull("@løsning") }
        .filterNot { _, value -> value["final"]?.asBoolean() == true }
        .peek { key, value ->
            log.info(
                "Mottok {} for behov med {}",
                keyValue("løsninger", value["@løsning"].fieldNames().asSequence().joinToString(", ")),
                keyValue("id", key)
            )
        }

private fun slåSammenLøsninger(behov1: JsonNode, behov2: JsonNode): ObjectNode {
    val result = behov1.deepCopy<ObjectNode>()
    val løsning = result["@løsning"] as ObjectNode
    behov2["@løsning"].fields().forEach { (key, value) ->
        løsning.set<ObjectNode>(key, value)
    }
    return result
}
