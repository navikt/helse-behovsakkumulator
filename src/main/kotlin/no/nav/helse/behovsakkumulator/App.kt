package no.nav.helse.behovsakkumulator

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.application.install
import io.ktor.metrics.micrometer.MicrometerMetrics
import io.ktor.routing.routing
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
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
        val server = embeddedServer(Netty, 8080) {
            install(MicrometerMetrics) {
                registry = meterRegistry
            }

            routing {
                registerHealthApi({ true }, { true }, meterRegistry)
            }
        }.start(wait = false)

        val akkumulator = Akkumulator()
        launchListeners(environment, serviceUser, akkumulator)

        Runtime.getRuntime().addShutdownHook(Thread {
            server.stop(10, 10, TimeUnit.SECONDS)
            applicationContext.close()
        })
    }
}

fun CoroutineScope.launchListeners(
    environment: Environment,
    serviceUser: ServiceUser,
    akkumulator: Akkumulator,
    baseConfig: Properties = loadBaseConfig(environment, serviceUser)
): Job {
    val behovProducer = KafkaProducer<String, JsonNode>(baseConfig.toProducerConfig())

    return listen<String, JsonNode>(environment.spleisBehovtopic, baseConfig.toConsumerConfig()) {
        // TODO: Filtrer ut behov som ikke skal behandles
        val behov = Behov(it.value())
        akkumulator.behandle(behov)
        akkumulator.løsning(behov.id)
            ?.let { løsning -> behovProducer.send(ProducerRecord<String, JsonNode>(environment.spleisBehovtopic, løsning.jsonNode)) }
    }
}


data class Behov(var jsonNode: JsonNode) {
    val id: String = jsonNode["@id"].textValue()
    val behov: Set<String> = jsonNode["behov"].map { it.textValue() }.toSet()
    val løsning: MutableMap<String, Any>? = jsonNode["@løsning"]?.let { objectMapper.convertValue(it, mutableMapOf<String, Any>().javaClass) }

    fun erKomplett() = behov.all { løsning?.containsKey(it) ?: false }
    fun oppdaterJsonNode() {
        (jsonNode as ObjectNode).replace("@løsning", objectMapper.valueToTree(løsning))
    }
}
