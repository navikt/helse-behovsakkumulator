package no.nav.helse.behovsakkumulator

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.rapids_and_rivers.test_support.TestRapid
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
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
        val behov4 = objectMapper.readTree("""{"@id": "${UUID.randomUUID()}", "@behovId": "behovsid5", "@opprettet": "${LocalDateTime.now()}", "vedtaksperiodeId": "id", "@behov": ["AndreYtelser"]}""")
        val løsning4 = behov4.medLøsning("""{ "AndreYtelser": { "felt1": null, "felt2": {}} }""")
        rapid.sendTestMessage(behov4.toString(), "behovsid5")
        rapid.sendTestMessage(løsning4, "behovsid5")

        assertEquals(1, rapid.inspektør.size)
        assertEquals("behovsid5", rapid.inspektør.key(0))
        assertTrue(rapid.inspektør.field(0, "@final").asBoolean())
        assertDoesNotThrow { LocalDateTime.parse(rapid.inspektør.field(0, "@besvart").asText()) }
        val løsninger = rapid.inspektør.field(0, "@løsning").fields().asSequence().toList()
        val løsningTyper = løsninger.map { it.key }
        assertTrue(løsningTyper.containsAll(listOf("AndreYtelser")))
        assertEquals(1, løsningTyper.size)
    }

    @Test
    fun `fler delsvar blir kombinert til et komplett svar`() {
        val behov1 =
            objectMapper.readTree("""{"@id": "${UUID.randomUUID()}", "@behovId": "behovsid1", "@opprettet": "${LocalDateTime.now()}", "vedtaksperiodeId": "id", "@behov": ["Sykepengehistorikk", "AndreYtelser", "Foreldrepenger"]}""")
        val løsning1 = behov1.medLøsning("""{ "Sykepengehistorikk": [] }""")
        val løsning2 = behov1.medLøsning("""{ "AndreYtelser": { "felt1": null, "felt2": {}} }""")
        val løsning3 = behov1.medLøsning("""{ "Foreldrepenger": {} }""")
        rapid.sendTestMessage(behov1.toString(), "behovsid1")
        rapid.sendTestMessage(løsning1, "behovsid1")
        rapid.sendTestMessage(løsning2, "behovsid1")
        rapid.sendTestMessage(løsning3, "behovsid1")

        val idLøsning3 = objectMapper.readTree(løsning3).path("@id").asText()
        assertEquals(1, rapid.inspektør.size)
        val løsninger = rapid.inspektør.field(0, "@løsning").fields().asSequence().toList()
        val løsningTyper = løsninger.map { it.key }
        assertTrue(løsningTyper.containsAll(listOf("Foreldrepenger", "AndreYtelser", "Sykepengehistorikk")))
        assertEquals(idLøsning3, rapid.inspektør.field(rapid.inspektør.size - 1, "@forårsaket_av").path("id").asText())
    }

    @Test
    fun `løser behov #3 uavhengig av om behov #2 er ferdigstilt`() {
        val behov2 =
            objectMapper.readTree("""{"@id": "${UUID.randomUUID()}", "@behovId": "behovsid2", "@opprettet": "${LocalDateTime.now()}", "vedtaksperiodeId": "id", "@behov": ["Sykepengehistorikk", "AndreYtelser", "Foreldrepenger"]}""")
        val løsning1ForBehov2 = behov2.medLøsning("""{ "Sykepengehistorikk": [] }""")
        val løsning2ForBehov2 = behov2.medLøsning("""{ "AndreYtelser": { "felt1": null, "felt2": {}} }""")

        val behov3 =
            objectMapper.readTree("""{"@id": "${UUID.randomUUID()}", "@behovId": "behovsid3", "@opprettet": "${LocalDateTime.now()}", "vedtaksperiodeId": "id", "@behov": ["Sykepengehistorikk", "AndreYtelser", "Foreldrepenger"]}""")
        val løsning1ForBehov3 = behov3.medLøsning("""{ "Sykepengehistorikk": [] }""")
        val løsning2ForBehov3 = behov3.medLøsning("""{ "AndreYtelser": { "felt1": null, "felt2": {}} }""")
        val løsning3ForBehov3 = behov3.medLøsning("""{ "Foreldrepenger": {} }""")
        rapid.sendTestMessage(behov2.toString(), "behovsid2")
        rapid.sendTestMessage(behov3.toString(), "behovsid3")
        rapid.sendTestMessage(løsning1ForBehov2, "behovsid2")
        rapid.sendTestMessage(løsning2ForBehov3, "behovsid3")
        rapid.sendTestMessage(løsning2ForBehov2, "behovsid2")
        rapid.sendTestMessage(løsning1ForBehov3, "behovsid3")
        rapid.sendTestMessage(løsning3ForBehov3, "behovsid3")

        assertEquals(1, rapid.inspektør.size)
        val record = rapid.inspektør.message(0)
        val løsninger = record["@løsning"].fields().asSequence().toList()
        val løsningTyper = løsninger.map { it.key }
        assertTrue(løsningTyper.containsAll(listOf("Foreldrepenger", "AndreYtelser", "Sykepengehistorikk")))
        assertEquals("behovsid3", record["@behovId"].asText())
    }

    @Test
    fun `overlever ugyldig json`() {
        val behovsid0 = UUID.randomUUID().toString()
        val behovsid1 = UUID.randomUUID().toString()

        val behov1 =
            """{
                    "@id": "${UUID.randomUUID()}",
                    "@behovId": "$behovsid1",
                    "@opprettet": "${LocalDateTime.now()}",
                    "vedtaksperiodeId": "id",
                    "@behov": ["Foreldrepenger"]
            }"""
        val løsning1 =
            """{
                "@id": "${UUID.randomUUID()}",
                "@behovId": "$behovsid1",
                "@opprettet": "${LocalDateTime.now()}",
                "vedtaksperiodeId": "id",
                "@behov": ["Foreldrepenger"],
                "@løsning": { "Foreldrepenger": [] }
            }""".trimMargin()

        rapid.sendTestMessage("THIS IS INVALID JSON", behovsid0)
        rapid.sendTestMessage(behov1, behovsid1)
        rapid.sendTestMessage(løsning1, behovsid1)

        assertEquals(1, rapid.inspektør.size)
    }

    @Test
    fun `publiserer endelig svar med samme key som siste melding`() {
        val behov = objectMapper.readTree("""{"@id": "${UUID.randomUUID()}", "@behovId": "en_behovId", "@opprettet": "${LocalDateTime.now()}", "vedtaksperiodeId": "id", "@behov": ["AndreYtelser", "HeltAndreYtelser"]}""")
        val løsning1 = behov.medLøsning("""{ "AndreYtelser": { "felt1": null, "felt2": {}} }""")
        val løsning2 = behov.medLøsning("""{ "HeltAndreYtelser": { "felt1": null, "felt2": {}} }""")
        rapid.sendTestMessage(behov.toString(), "behov_nøkkel")
        rapid.sendTestMessage(løsning1, "behov_nøkkel")
        // I virkeligheten skjer det neppe at en løsning kommer på en annen key enn behovet, dette er bare for å vise
        // hva aktuell oppførsel er
        val usannsynligKey = "behov_nøkkel_sist"
        rapid.sendTestMessage(løsning2, usannsynligKey)

        assertEquals(1, rapid.inspektør.size)
        assertEquals(usannsynligKey, rapid.inspektør.key(0))
        assertTrue(rapid.inspektør.field(0, "@final").asBoolean())
    }

    @Test
    fun `publiserer behov_uten_fullstendig_løsning med en annen key enn mottatt melding`() {
        val behovId_somIkkeBlirKomplett = "en_behovId"
        val behov = objectMapper.readTree("""{"@id": "${UUID.randomUUID()}", "@behovId": "$behovId_somIkkeBlirKomplett", "@opprettet": "${LocalDateTime.now().minusMinutes(31)}", "vedtaksperiodeId": "id", "@behov": ["AndreYtelser", "NoenAndreYtelser", "HeltAndreYtelser"]}""")
        val løsning1 = behov.medLøsning("""{ "AndreYtelser": { "felt1": null, "felt2": {}} }""")
        val løsningBehov2 =
            objectMapper.readTree("""{"@id": "${UUID.randomUUID()}", "@behovId": "en_annen_behovId", "@opprettet": "${LocalDateTime.now()}", "vedtaksperiodeId": "id2", "@behov": ["AndreYtelser", "NoenAndreYtelser"]}""")
                .medLøsning("""{ "AndreYtelser": { "felt1": null, "felt2": {}} }""")
        rapid.sendTestMessage(behov.toString(), "behov_nøkkel")
        rapid.sendTestMessage(løsning1, "behov_nøkkel")
        rapid.sendTestMessage(løsningBehov2, "behov_nøkkel")

        assertEquals(1, rapid.inspektør.size)
        assertNotEquals("behov_nøkkel", rapid.inspektør.key(0))
        assertEquals(behovId_somIkkeBlirKomplett, rapid.inspektør.key(0))
        assertEquals("behov_uten_fullstendig_løsning", rapid.inspektør.field(0, "@event_name").asText())
    }

    private fun JsonNode.medLøsning(løsning: String) =
        (this.deepCopy() as ObjectNode).apply {
            put("@id", UUID.randomUUID().toString())
            set<ObjectNode>("@løsning", objectMapper.readTree(løsning))
        }.toString()
}
