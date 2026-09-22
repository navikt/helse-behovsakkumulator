package no.nav.helse.behovsakkumulator

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers.isMissingOrNull
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import no.nav.sykepenger.libs.logging.MdcKey
import no.nav.sykepenger.libs.logging.loggError
import no.nav.sykepenger.libs.logging.loggInfo
import no.nav.sykepenger.libs.logging.medMdc
import tools.jackson.databind.JsonNode
import tools.jackson.databind.node.ObjectNode
import java.time.LocalDateTime

class Behovsakkumulator(
    rapidsConnection: RapidsConnection,
    private val repository: BehovRepository,
) : River.PacketListener {
    init {
        River(rapidsConnection)
            .apply {
                precondition {
                    it.requireKey("@behov")
                    it.requireKey("@løsning")
                    it.forbid("@final")
                }
                validate {
                    it.requireKey("@id")
                    it.interestedIn("@behovId")
                    it.interestedIn("vedtaksperiodeId")
                    it.require("@opprettet", JsonNode::asLocalDateTime)
                }
            }.register(this)
    }

    override fun onError(
        problems: MessageProblems,
        context: MessageContext,
        metadata: MessageMetadata,
    ) {
        loggError("Forstår ikke behov", "problemer" to problems.toExtendedReport())
    }

    override fun onPacket(
        packet: JsonMessage,
        context: MessageContext,
        metadata: MessageMetadata,
        meterRegistry: MeterRegistry,
    ) {
        val packetAsJson = objectMapper.readTree(packet.toJson()) as ObjectNode
        val id = packetAsJson.behovId()

        medMdc(
            MdcKey.MELDING_ID to
                packetAsJson["@id"]
                    .takeUnless { it.isMissingOrNull() }
                    ?.asString(),
            MdcKey.VEDTAKSPERIODE_ID to
                packetAsJson.path("vedtaksperiodeId")
                    .takeUnless { it.isMissingOrNull() }
                    ?.asString(),
        ) {
            loggInfo(
                "Mottok behov",
                "løsninger" to packetAsJson["@løsning"].feltnavn().prettyPrint<String>(),
                "behovId" to id,
            )

            val resultat = repository.hent(id)?.kombinerLøsninger(packetAsJson) ?: packetAsJson

            if (resultat.erKomplett()) {
                resultat.put("@final", true)
                resultat.put("@besvart", LocalDateTime.now().toString())
                loggInfo("Markert behov som final", "behovId" to id)
                context.publish(objectMapper.writeValueAsString(resultat))
                repository.fjern(id)
            } else {
                repository.lagre(id, resultat)
            }
        }
    }

    private fun JsonNode.erKomplett(): Boolean {
        val løsninger = this["@løsning"].feltnavn()
        val behov = this["@behov"].toList().map(JsonNode::asString)
        return behov.all { it in løsninger }
    }

    private fun ObjectNode.kombinerLøsninger(packet: JsonNode): ObjectNode {
        val løsning = this["@løsning"] as ObjectNode
        packet["@løsning"].properties().forEach { (behovtype, delløsning) ->
            løsning.set(behovtype, delløsning)
        }
        val løsninger = this["@løsning"].feltnavn()
        this@Behovsakkumulator.loggInfo(
            "Satt sammen løsninger for behov",
            "løsninger" to løsninger.prettyPrint<String>(),
            "behovId" to behovId(),
            "forespurte_behov" to this["@behov"].prettyPrint<JsonNode>(),
            "manglende_behov" to this["@behov"].filter { it.asString() !in løsninger }.prettyPrint<JsonNode>(),
        )
        return this
    }

    private fun JsonNode.behovId() =
        this["@behovId"].takeUnless { it.isMissingOrNull() }?.asString() ?: this["@id"].asString().also {
            this@Behovsakkumulator.loggInfo("Akkumulerer behov basert på gammel metode vha @id")
        }

    private fun JsonNode.feltnavn() = propertyNames().asIterable()

    @Suppress("UNCHECKED_CAST")
    private inline fun <reified T> Iterable<T>.prettyPrint() =
        when (T::class) {
            JsonNode::class -> (this as Iterable<JsonNode>).map(JsonNode::asString)
            String::class -> this
            else -> throw UnsupportedOperationException()
        }.joinToString(prefix = "[", postfix = "]")
}
