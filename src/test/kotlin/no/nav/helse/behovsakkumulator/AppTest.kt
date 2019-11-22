package no.nav.helse.behovsakkumulator

import com.fasterxml.jackson.databind.JsonNode
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import no.nav.common.KafkaEnvironment
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.nio.file.Files
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class AppTest : CoroutineScope {
    override val coroutineContext: CoroutineContext = Executors.newFixedThreadPool(4).asCoroutineDispatcher()

    val kafkaStreamsStateDir = Files.createTempDirectory("kafka-streams-test")
    fun deleteDir() {
        if (Files.exists(kafkaStreamsStateDir)) {
            Files.walk(kafkaStreamsStateDir).sorted(Comparator.reverseOrder()).forEach { Files.delete(it) }
        }
    }

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

    private val serviceUser = ServiceUser("user", "password")
    private val environment = Environment(
        kafkaBootstrapServers = embeddedKafkaEnvironment.brokersURL,
        spleisBehovtopic = testTopic
    )
    private val testKafkaProperties = loadBaseConfig(environment, serviceUser).apply {
        this[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = "PLAINTEXT"
        this[StreamsConfig.COMMIT_INTERVAL_MS_CONFIG] = 100
        this[SaslConfigs.SASL_MECHANISM] = "PLAIN"
    }

    private lateinit var stream: KafkaStreams

    private val behovProducer = KafkaProducer<String, JsonNode>(testKafkaProperties.toProducerConfig())
    private val behovConsumer = KafkaConsumer<String, JsonNode>(testKafkaProperties.toConsumerConfig().also {
        it[ConsumerConfig.GROUP_ID_CONFIG] = "noefornuftigværsåsnill"
    }).also {
        it.subscribe(listOf(testTopic))
    }


    @BeforeAll
    fun setup() {
        embeddedKafkaEnvironment.start()
        stream = createStream(environment, serviceUser, testKafkaProperties.also {
            it[StreamsConfig.STATE_DIR_CONFIG] = kafkaStreamsStateDir.toAbsolutePath().toString()
        })
        stream.start()
    }

    @Test
    fun `fler delsvar blir kombinert til et komplett svar`() {
        val behov1 =
            objectMapper.readTree("""{"@id": "behovsid1", "aktørId": "aktørid1", "behov": ["Sykepengehistorikk", "AndreYtelser", "Foreldrepenger"]}""")
        val løsning1 =
            objectMapper.readTree("""{"@id": "behovsid1", "aktørId": "aktørid1", "behov": ["Sykepengehistorikk", "AndreYtelser", "Foreldrepenger"], "@løsning": { "Sykepengehistorikk": [] } }""")
        val løsning2 =
            objectMapper.readTree("""{"@id": "behovsid1", "aktørId": "aktørid1", "behov": ["Sykepengehistorikk", "AndreYtelser", "Foreldrepenger"], "@løsning": { "AndreYtelser": { "felt1": null, "felt2": {}} } }""")
        val løsning3 =
            objectMapper.readTree("""{"@id": "behovsid1", "aktørId": "aktørid1", "behov": ["Sykepengehistorikk", "AndreYtelser", "Foreldrepenger"], "@løsning": { "Foreldrepenger": {} }}""")
        behovProducer.send(ProducerRecord(testTopic, "behovsid1", behov1))
        behovProducer.send(ProducerRecord(testTopic, "behovsid1", løsning1))
        behovProducer.send(ProducerRecord(testTopic, "behovsid1", løsning2))
        behovProducer.send(ProducerRecord(testTopic, "behovsid1", løsning3))

        mutableListOf<ConsumerRecord<String, JsonNode>>().also { records ->
            await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted {
                    records.addAll(behovConsumer.poll(Duration.ofMillis(100)).toList())
                    val finalRecords = records.map { it.value() }.filter { it["final"]?.asBoolean() ?: false }
                    assertEquals(1, finalRecords.size)
                    val løsninger = finalRecords.first()["@løsning"].fields().asSequence().toList()
                    val løsningTyper = løsninger.map { it.key }
                    assertTrue(løsningTyper.containsAll(listOf("Foreldrepenger", "AndreYtelser", "Sykepengehistorikk")))
                }
        }
        behovConsumer.poll(Duration.ofMillis(1000))
    }

    @Test
    fun `løser behov #3 uavhengig av om behov #2 er ferdigstilt`() {
        val behov2 =
            objectMapper.readTree("""{"@id": "behovsid2", "aktørId": "aktørid1", "behov": ["Sykepengehistorikk", "AndreYtelser", "Foreldrepenger"]}""")
        val løsning1ForBehov2 =
            objectMapper.readTree("""{"@id": "behovsid2", "aktørId": "aktørid1", "behov": ["Sykepengehistorikk", "AndreYtelser", "Foreldrepenger"], "@løsning": { "Sykepengehistorikk": [] } }""")
        val løsning2ForBehov2 =
            objectMapper.readTree("""{"@id": "behovsid2", "aktørId": "aktørid1", "behov": ["Sykepengehistorikk", "AndreYtelser", "Foreldrepenger"], "@løsning": { "AndreYtelser": { "felt1": null, "felt2": {}} } }""")
        val behov3 =
            objectMapper.readTree("""{"@id": "behovsid3", "aktørId": "aktørid1", "behov": ["Sykepengehistorikk", "AndreYtelser", "Foreldrepenger"]}""")
        val løsning1ForBehov3 =
            objectMapper.readTree("""{"@id": "behovsid3", "aktørId": "aktørid1", "behov": ["Sykepengehistorikk", "AndreYtelser", "Foreldrepenger"], "@løsning": { "Sykepengehistorikk": [] } }""")
        val løsning2ForBehov3 =
            objectMapper.readTree("""{"@id": "behovsid3", "aktørId": "aktørid1", "behov": ["Sykepengehistorikk", "AndreYtelser", "Foreldrepenger"], "@løsning": { "AndreYtelser": { "felt1": null, "felt2": {}} } }""")
        val løsning3ForBehov3 =
            objectMapper.readTree("""{"@id": "behovsid3", "aktørId": "aktørid1", "behov": ["Sykepengehistorikk", "AndreYtelser", "Foreldrepenger"], "@løsning": { "Foreldrepenger": {} }}""")
        behovProducer.send(ProducerRecord(testTopic, "behovsid2", behov2))
        behovProducer.send(ProducerRecord(testTopic, "behovsid3", behov3))
        behovProducer.send(ProducerRecord(testTopic, "behovsid2", løsning1ForBehov2))
        behovProducer.send(ProducerRecord(testTopic, "behovsid3", løsning2ForBehov3))
        behovProducer.send(ProducerRecord(testTopic, "behovsid2", løsning2ForBehov2))
        behovProducer.send(ProducerRecord(testTopic, "behovsid3", løsning1ForBehov3))
        behovProducer.send(ProducerRecord(testTopic, "behovsid3", løsning3ForBehov3))

        mutableListOf<ConsumerRecord<String, JsonNode>>().also { records ->
            await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted {
                    records.addAll(behovConsumer.poll(Duration.ofMillis(100)).toList())
                    val finalRecords = records.map { it.value() }.filter { it["final"]?.asBoolean() ?: false }
                    assertEquals(1, finalRecords.size)
                    val record = finalRecords.first()
                    val løsninger = record["@løsning"].fields().asSequence().toList()
                    val løsningTyper = løsninger.map { it.key }
                    assertTrue(løsningTyper.containsAll(listOf("Foreldrepenger", "AndreYtelser", "Sykepengehistorikk")))
                    assertEquals("behovsid3", record["@id"].asText())
                }
        }
    }

    /*@Test
    fun `bruker sist ankomne svar i tilfelle duplikatsvar`() {
        val behov4 =
            objectMapper.readTree("""{"@id": "behovsid4", "aktørId": "aktørid1", "behov": ["Sykepengehistorikk", "AndreYtelser"]}""")
        val løsning1ForBehov4 =
            objectMapper.readTree("""{"@id": "behovsid4", "aktørId": "aktørid1", "behov": ["Sykepengehistorikk", "AndreYtelser"], "@løsning": { "Sykepengehistorikk": [] } }""")
        val løsning2ForBehov4 =
            objectMapper.readTree("""{"@id": "behovsid4", "aktørId": "aktørid1", "behov": ["Sykepengehistorikk", "AndreYtelser"], "@løsning": { "AndreYtelser": { "felt1": "første verdi"} } }""")
        val duplikatløsning2ForBehov4 =
            objectMapper.readTree("""{"@id": "behovsid4", "aktørId": "aktørid1", "behov": ["Sykepengehistorikk", "AndreYtelser"], "@løsning": { "AndreYtelser": { "felt1": "andre verdi"} } }""")

        behovProducer.send(ProducerRecord(testTopic, "behovsid4", behov4))
        behovProducer.send(ProducerRecord(testTopic, "behovsid4", løsning1ForBehov4))
        behovProducer.send(ProducerRecord(testTopic, "behovsid4", løsning2ForBehov4))

        Thread.sleep(10000)
        behovProducer.send(ProducerRecord(testTopic, "behovsid4", duplikatløsning2ForBehov4))

        Thread.sleep(10000)

        mutableListOf<ConsumerRecord<String, JsonNode>>().also { records ->
            await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted {
                    records.addAll(behovConsumer.poll(Duration.ofMillis(100)).toList())
                    val finalRecords = records.map { it.value() }.filter { it["final"]?.asBoolean() ?: false }
                    assertEquals(1, finalRecords.size)
                    val record = finalRecords.first()
                    val løsninger = record["@løsning"].fields().asSequence().toList()

                    assertEquals("andre verdi", record["@løsning"]["AndreYtelser"]["felt1"].asText())
                }
        }
    }*/

    @AfterAll
    fun tearDown() {
        stream.close()
        embeddedKafkaEnvironment.close()
        deleteDir()
    }
}
