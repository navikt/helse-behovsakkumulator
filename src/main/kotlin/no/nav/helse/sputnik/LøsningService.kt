package no.nav.helse.sputnik

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode

class LøsningService(private val fpsakRestClient: FpsakRestClient) {
    fun løsBehov(behov: JsonNode): JsonNode = behov.deepCopy<ObjectNode>()
        .set("@løsning", objectMapper.valueToTree(hentYtelser(behov)))

    private fun hentYtelser(behov: JsonNode): Løsning {
        val aktørId = behov["aktørId"].asText()
        val behovId = behov["@id"].asText()

        val foreldrepengeytelse = fpsakRestClient.hentGjeldendeForeldrepengeytelse(aktørId)
        log.info("hentet gjeldende foreldrepengeytelse for behov: $behovId")
        val svangerskapsytelse = fpsakRestClient.hentGjeldendeSvangerskapsytelse(aktørId)
        log.info("hentet gjeldende svangerskapspengeytelse for behov: $behovId")

        return Løsning(foreldrepengeytelse, svangerskapsytelse)
    }
}

data class Løsning(
    val foreldrepengeytelse: Foreldrepengeytelse? = null,
    val svangerskapsytelse: Svangerskapsytelse? = null
)
