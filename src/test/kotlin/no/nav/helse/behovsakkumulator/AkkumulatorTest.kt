package no.nav.helse.behovsakkumulator

import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*

internal class AkkumulatorTest {

    companion object {

        val behov1 =
            objectMapper.readTree("""{"@id": "behovsid1", "aktørId":"aktørid1", "behov": ["Sykepengehistorikk", "AndreYtelser"]}""")
        val løsning1 =
            objectMapper.readTree("""{"@id": "behovsid1", "aktørId":"aktørid1", "behov": ["Sykepengehistorikk", "AndreYtelser"], "@løsning": { "Sykepengehistorikk": [] } }""")
        val løsning2 =
            objectMapper.readTree("""{"@id": "behovsid1", "aktørId":"aktørid1", "behov": ["Sykepengehistorikk", "AndreYtelser"], "@løsning": { "AndreYtelser": { "felt1": null, "felt2": {}} } }""")
    }

    @Test
    fun `ubesvarte behov`() {
        val akkumulator = Akkumulator()
        akkumulator.behandle(Behov(behov1))
        assertEquals(1, akkumulator.ubesvarteBehov())
    }

    @Test
    fun `behandler et svar`() {
        val akkumulator = Akkumulator()
        akkumulator.behandle(Behov(behov1))
        akkumulator.behandle(Behov(løsning1))

        val behovMedLøsning = akkumulator.løsning(Behov(behov1).id)!!

        assertNotNull(behovMedLøsning)
        assertTrue(behovMedLøsning.jsonNode["@løsning"]["Sykepengehistorikk"].isArray)
        assertEquals(0, behovMedLøsning.jsonNode["@løsning"]["Sykepengehistorikk"].size())
    }

    @Test
    fun `behandler begge svarene på et behov`() {
        val akkumulator = Akkumulator()
        akkumulator.behandle(Behov(behov1))
        akkumulator.behandle(Behov(løsning1))
        akkumulator.behandle(Behov(løsning2))

        val behovMedLøsning = akkumulator.løsning(Behov(behov1).id)!!

        assertNotNull(behovMedLøsning)
        assertTrue(behovMedLøsning.jsonNode["@løsning"]["Sykepengehistorikk"].isArray)
        assertEquals(0, behovMedLøsning.jsonNode["@løsning"]["Sykepengehistorikk"].size())
        assertTrue(behovMedLøsning.jsonNode["@løsning"]["AndreYtelser"].isObject)
    }

    @Test
    fun `første svar på behov er ikke komplett, men andre er det`() {
        val akkumulator = Akkumulator()
        akkumulator.behandle(Behov(behov1))
        akkumulator.behandle(Behov(løsning1))

        val behovUtenLøsning = akkumulator.løsning(Behov(behov1).id)!!

        assertNull(behovUtenLøsning)

        akkumulator.behandle(Behov(løsning2))

        val behovMedLøsning = akkumulator.løsning(Behov(behov1).id)!!

        assertNotNull(behovMedLøsning)
        assertTrue(behovMedLøsning.jsonNode["@løsning"]["Sykepengehistorikk"].isArray)
        assertEquals(0, behovMedLøsning.jsonNode["@løsning"]["Sykepengehistorikk"].size())
        assertTrue(behovMedLøsning.jsonNode["@løsning"]["AndreYtelser"].isObject)
    }
}