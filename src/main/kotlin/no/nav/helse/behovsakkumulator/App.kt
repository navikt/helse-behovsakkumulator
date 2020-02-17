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
import kotlinx.coroutines.*
import net.logstash.logback.argument.StructuredArguments.keyValue
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Produced
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

val meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
val objectMapper: ObjectMapper = jacksonObjectMapper()
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    .registerModule(JavaTimeModule())
val log = LoggerFactory.getLogger("behovsakkumulator")
val sikkerLog = LoggerFactory.getLogger("tjenestekall")

data class ApplicationState(var ready: Boolean = false, var alive: Boolean = true)

fun main() = runBlocking {
    val serviceUser = readServiceUserCredentials()
    val environment = setUpEnvironment()

    launchApplication(environment, serviceUser)
}

fun launchApplication(
    environment: Environment,
    serviceUser: ServiceUser
) {
    val applicationState = ApplicationState()
    val applicationContext = Executors.newFixedThreadPool(4).asCoroutineDispatcher()
    val exceptionHandler = CoroutineExceptionHandler { context, e ->
        log.error("Feil i lytter", e)
        context.cancel(CancellationException("Feil i lytter", e))
    }
    runBlocking(exceptionHandler + applicationContext) {
        val server = embeddedServer(Netty, 8080) {
            install(MicrometerMetrics) {
                registry = meterRegistry
            }

            routing {
                registerHealthApi(
                    liveness = { applicationState.alive },
                    readiness = { applicationState.ready },
                    meterRegistry = meterRegistry
                )
            }
        }.start(wait = false)

        val stream = createStream(environment, serviceUser)

        stream.setStateListener { newState, _ ->
            applicationState.ready = newState.isRunning
            applicationState.alive = newState != KafkaStreams.State.NOT_RUNNING
        }
        stream.start()

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
            environment.spleisRapidtopic,
            Consumed.with(Serdes.StringSerde(), JacksonKafkaSerde())
        )
        .alleBehovSomIkkeErMarkertFerdig()
        .kombinerDelløsningerPåBehov()
        .bareKomplettLøsningPåBehov()
        .markerBehovFerdig()
        .to(environment.spleisRapidtopic, Produced.with(Serdes.StringSerde(), JacksonKafkaSerde()))
    return KafkaStreams(builder.build(), baseConfig.toStreamsConfig()).apply {
        setUncaughtExceptionHandler { _, err ->
            log.error("Caught exception in stream: ${err.message}", err)
            close()
        }
    }
}

private fun KStream<String, JsonNode>.markerBehovFerdig(): KStream<String, JsonNode> =
    this.mapValues { value ->
        (value as ObjectNode)
            .put("@final", true)
            .put("@besvart", LocalDateTime.now().toString()) as JsonNode
    }.peek { _, value ->
        log.info(
            "Markert behov med {} ({}) som final",
            keyValue("id", value["@id"].asText()),
            keyValue("vedtaksperiodeId", value.path("vedtaksperiodeId").asText("IKKE_SATT"))
        )
        sikkerLog.info(
            "Markert behov med {} ({}) som final",
            keyValue("id", value["@id"].asText()),
            keyValue("vedtaksperiodeId", value.path("vedtaksperiodeId").asText("IKKE_SATT"))
        )
    }

private fun KStream<String, JsonNode>.kombinerDelløsningerPåBehov(): KStream<String, JsonNode> =
    this.groupBy({ _, value ->
        value.path("@id").asText()
    }, Grouped.with(Serdes.String(), JacksonKafkaSerde()))
        .reduce(::slåSammenLøsninger)
        .toStream()
        .peek { _, value ->
            log.info(
                "Satt sammen {} for behov med id {} ({}). Forventer {}",
                keyValue("løsninger", value["@løsning"].fieldNames().asSequence().joinToString(", ")),
                keyValue("id", value["@id"].asText()),
                keyValue("vedtaksperiodeId", value.path("vedtaksperiodeId").asText("IKKE_SATT")),
                keyValue("behov", value["@behov"].asSequence().map(JsonNode::asText).joinToString(", "))
            )
            sikkerLog.info(
                "Satt sammen {} for behov med id {} ({}). Forventer {}",
                keyValue("løsninger", value["@løsning"].fieldNames().asSequence().joinToString(", ")),
                keyValue("id", value["@id"].asText()),
                keyValue("vedtaksperiodeId", value.path("vedtaksperiodeId").asText("IKKE_SATT")),
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
        .filterNot { _, value -> value["@final"]?.asBoolean() == true }
        .peek { _, value ->
            log.info(
                "Mottok {} for behov med {} ({})",
                keyValue("løsninger", value["@løsning"].fieldNames().asSequence().joinToString(", ")),
                keyValue("id", value["@id"].asText()),
                keyValue("vedtaksperiodeId", value.path("vedtaksperiodeId").asText("IKKE_SATT"))
            )
            sikkerLog.info(
                "Mottok {} for behov med {} ({})",
                keyValue("løsninger", value["@løsning"].fieldNames().asSequence().joinToString(", ")),
                keyValue("id", value["@id"].asText()),
                keyValue("vedtaksperiodeId", value.path("vedtaksperiodeId").asText("IKKE_SATT"))
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
