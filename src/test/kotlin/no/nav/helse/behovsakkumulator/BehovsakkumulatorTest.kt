package no.nav.helse.behovsakkumulator

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import no.nav.helse.rapids_rivers.RapidsConnection
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertDoesNotThrow
import java.time.LocalDateTime
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class BehovsakkumulatorTest {
    private companion object {
        private val objectMapper: ObjectMapper = jacksonObjectMapper()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .registerModule(JavaTimeModule())
    }

    private lateinit var rapid: TestRapid

    @BeforeEach
    fun setup() {
        rapid = TestRapid()
        Behovsakkumulator(rapid)
    }

    @Test
    fun `frittstående svar blir markert final`() {
        val behov4 = objectMapper.readTree("""{"@id": "behovsid5", "vedtaksperiodeId": "id", "@behov": ["AndreYtelser"]}""")
        val løsning4 = behov4.medLøsning("""{ "AndreYtelser": { "felt1": null, "felt2": {}} }""")
        rapid.sendTestMessage("behovsid5", behov4.toString())
        rapid.sendTestMessage("behovsid5", løsning4)

        assertEquals(1, rapid.sentMessages.size)
        assertEquals("behovsid5", rapid.sentMessages.first().first)
        assertTrue(rapid.sentMessages.first().second["@final"].asBoolean())
        assertDoesNotThrow { LocalDateTime.parse(rapid.sentMessages.first().second["@besvart"].asText()) }
        val løsninger = rapid.sentMessages.first().second["@løsning"].fields().asSequence().toList()
        val løsningTyper = løsninger.map { it.key }
        assertTrue(løsningTyper.containsAll(listOf("AndreYtelser")))
        assertEquals(1, løsningTyper.size)
    }

    @Test
    fun `fler delsvar blir kombinert til et komplett svar`() {
        val behov1 =
            objectMapper.readTree("""{"@id": "behovsid1", "vedtaksperiodeId": "id", "@behov": ["Sykepengehistorikk", "AndreYtelser", "Foreldrepenger"]}""")
        val løsning1 = behov1.medLøsning("""{ "Sykepengehistorikk": [] }""")
        val løsning2 = behov1.medLøsning("""{ "AndreYtelser": { "felt1": null, "felt2": {}} }""")
        val løsning3 = behov1.medLøsning("""{ "Foreldrepenger": {} }""")
        rapid.sendTestMessage("behovsid1", behov1.toString())
        rapid.sendTestMessage("behovsid1", løsning1)
        rapid.sendTestMessage("behovsid1", løsning2)
        rapid.sendTestMessage("behovsid1", løsning3)

        assertEquals(1, rapid.sentMessages.size)
        val løsninger = rapid.sentMessages.first().second["@løsning"].fields().asSequence().toList()
        val løsningTyper = løsninger.map { it.key }
        assertTrue(løsningTyper.containsAll(listOf("Foreldrepenger", "AndreYtelser", "Sykepengehistorikk")))
    }

    @Test
    fun `løser behov #3 uavhengig av om behov #2 er ferdigstilt`() {
        val behov2 =
            objectMapper.readTree("""{"@id": "behovsid2", "vedtaksperiodeId": "id", "@behov": ["Sykepengehistorikk", "AndreYtelser", "Foreldrepenger"]}""")
        val løsning1ForBehov2 = behov2.medLøsning("""{ "Sykepengehistorikk": [] }""")
        val løsning2ForBehov2 = behov2.medLøsning("""{ "AndreYtelser": { "felt1": null, "felt2": {}} }""")

        val behov3 =
            objectMapper.readTree("""{"@id": "behovsid3", "vedtaksperiodeId": "id", "@behov": ["Sykepengehistorikk", "AndreYtelser", "Foreldrepenger"]}""")
        val løsning1ForBehov3 = behov3.medLøsning("""{ "Sykepengehistorikk": [] }""")
        val løsning2ForBehov3 = behov3.medLøsning("""{ "AndreYtelser": { "felt1": null, "felt2": {}} }""")
        val løsning3ForBehov3 = behov3.medLøsning("""{ "Foreldrepenger": {} }""")
        rapid.sendTestMessage("behovsid2", behov2.toString())
        rapid.sendTestMessage("behovsid3", behov3.toString())
        rapid.sendTestMessage("behovsid2", løsning1ForBehov2)
        rapid.sendTestMessage("behovsid3", løsning2ForBehov3)
        rapid.sendTestMessage("behovsid2", løsning2ForBehov2)
        rapid.sendTestMessage("behovsid3", løsning1ForBehov3)
        rapid.sendTestMessage("behovsid3", løsning3ForBehov3)

        assertEquals(1, rapid.sentMessages.size)
        val record = rapid.sentMessages.first().second
        val løsninger = record["@løsning"].fields().asSequence().toList()
        val løsningTyper = løsninger.map { it.key }
        assertTrue(løsningTyper.containsAll(listOf("Foreldrepenger", "AndreYtelser", "Sykepengehistorikk")))
        assertEquals("behovsid3", record["@id"].asText())
    }

    @Test
    fun `overlever ugyldig json`() {
        val behovsid0 = UUID.randomUUID().toString()
        val behovsid1 = UUID.randomUUID().toString()

        val behov1 =
            """{
                    "@id": "$behovsid1",
                    "vedtaksperiodeId": "id",
                    "@behov": ["Foreldrepenger"]
            }"""
        val løsning1 =
            """{
                "@id": "$behovsid1",
                "vedtaksperiodeId": "id",
                "@behov": ["Foreldrepenger"],
                "@løsning": { "Foreldrepenger": [] }
            }""".trimMargin()

        rapid.sendTestMessage(behovsid0, "THIS IS INVALID JSON")
        rapid.sendTestMessage(behovsid1, behov1)
        rapid.sendTestMessage(behovsid1, løsning1)

        assertEquals(1, rapid.sentMessages.size)
    }

    private fun JsonNode.medLøsning(løsning: String) =
        (this.deepCopy() as ObjectNode).set<ObjectNode>("@løsning", objectMapper.readTree(løsning)).toString()

    private class TestRapid : RapidsConnection() {
        val sentMessages = mutableListOf<Pair<String, JsonNode>>()

        fun sendTestMessage(key: String, message: String) {
            val context = TestContext(key)
            listeners.forEach { it.onMessage(message, context) }
        }

        override fun publish(message: String) {}
        override fun publish(key: String, message: String) {}
        override fun start() {}
        override fun stop() {}

        private inner class TestContext(private val originalKey: String) : MessageContext {
            override fun send(message: String) {
                send(originalKey, message)
            }

            override fun send(key: String, message: String) {
                sentMessages.add(key to objectMapper.readTree(message))
            }
        }
    }
}
