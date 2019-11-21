package no.nav.helse.behovsakkumulator

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.treeToValue
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.asCoroutineDispatcher
import no.nav.common.KafkaEnvironment
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.config.SaslConfigs
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class AppTest : CoroutineScope {
    override val coroutineContext: CoroutineContext = Executors.newFixedThreadPool(4).asCoroutineDispatcher()

    private val testTopic = "privat-helse-sykepenger-behov"
    private val topicInfos = listOf(
        KafkaEnvironment.TopicInfo(testTopic)
    )

    private val embeddedKafkaEnvironment = KafkaEnvironment(
        autoStart = false,
        noOfBrokers = 1,
        topicInfos = topicInfos,
        withSchemaRegistry = false,
        withSecurity = false
    )

    private val akkumulator = Akkumulator()

    private val serviceUser = ServiceUser("user", "password")
    private val environment = Environment(
        kafkaBootstrapServers = embeddedKafkaEnvironment.brokersURL,
        spleisBehovtopic = testTopic
    )
    private val testKafkaProperties = loadBaseConfig(environment, serviceUser).apply {
        this[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = "PLAINTEXT"
        this[SaslConfigs.SASL_MECHANISM] = "PLAIN"
    }

    private lateinit var job: Job

    private val behovProducer = KafkaProducer<String, JsonNode>(testKafkaProperties.toProducerConfig())
    private val behovConsumer = KafkaConsumer<String, JsonNode>(testKafkaProperties.toConsumerConfig().also {
        it[ConsumerConfig.GROUP_ID_CONFIG] = "noefornuftigværsåsnill"
    }).also {
        it.subscribe(listOf(testTopic))
    }


    @BeforeAll
    fun setup() {
        embeddedKafkaEnvironment.start()
        job = launchListeners(environment, serviceUser, akkumulator, testKafkaProperties)
    }

    @Test
    fun `skal lese et behov og legge det i minnet`() {
        val behov1 =
            """{"@id": "behovsid1", "aktørId":"aktørid1", "behov": ["Sykepengehistorikk", "AnsattINav", "AndreYtelser"]}"""
        behovProducer.send(ProducerRecord(testTopic, "123", objectMapper.readValue(behov1)))

        mutableListOf<ConsumerRecord<String, JsonNode>>().apply {
            await()
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted {
                    addAll(behovConsumer.poll(Duration.ofMillis(100)).toList())
                    assertEquals(1, akkumulator.ubesvarteBehov())
                }
        }
    }

    @AfterAll
    fun tearDown() {
        job.cancel()
        embeddedKafkaEnvironment.close()
    }
}
