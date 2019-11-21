package no.nav.helse.behovsakkumulator

import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*

internal class AkkumulatorTest {

    companion object {

        val behov1 =
            Behov(objectMapper.readTree("""{"@id": "behovsid1", "aktørId":"aktørid1", "behov": ["Sykepengehistorikk", "AndreYtelser"]}"""))
        val løsning1 =
            Behov(objectMapper.readTree("""{"@id": "behovsid1", "aktørId":"aktørid1", "behov": ["Sykepengehistorikk"], "@løsning": { "Sykepengehistorikk": [] } }"""))
        val løsning2 =
            Behov(objectMapper.readTree("""{"@id": "behovsid1", "aktørId":"aktørid1", "behov": ["Sykepengehistorikk"], "@løsning": { "AndreYtelser": { "felt1": null, "felt2": {}} } }"""))
    }

    @Test
    fun `ubesvarte behov`() {
        val akkumulator = Akkumulator()
        akkumulator.behandle(behov1)
        assertEquals(1, akkumulator.ubesvarteBehov())
    }

    @Test
    fun `behandler et svar`() {
        val akkumulator = Akkumulator()
        akkumulator.behandle(behov1)
        akkumulator.behandle(løsning1)

        val behovMedLøsning = akkumulator.løsning(behov1.id)!!

        assertNotNull(behovMedLøsning)
        assertTrue(behovMedLøsning.jsonNode["@løsning"]["Sykepengehistorikk"].isArray)
        assertEquals(0, behovMedLøsning.jsonNode["@løsning"]["Sykepengehistorikk"].size())
    }

    @Test
    fun `behandler begge svarene på et behov`() {
        val akkumulator = Akkumulator()
        akkumulator.behandle(behov1)
        akkumulator.behandle(løsning1)
        akkumulator.behandle(løsning2)

        val behovMedLøsning = akkumulator.løsning(behov1.id)!!

        assertNotNull(behovMedLøsning)
        assertTrue(behovMedLøsning.jsonNode["@løsning"]["Sykepengehistorikk"].isArray)
        assertEquals(0, behovMedLøsning.jsonNode["@løsning"]["Sykepengehistorikk"].size())
        assertTrue(behovMedLøsning.jsonNode["@løsning"]["AndreYtelser"].isObject)
    }
}