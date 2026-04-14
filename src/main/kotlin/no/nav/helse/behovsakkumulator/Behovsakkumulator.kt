package no.nav.helse.behovsakkumulator

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import java.time.LocalDateTime
import net.logstash.logback.argument.StructuredArguments.keyValue
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class Behovsakkumulator(rapidsConnection: RapidsConnection, private val repository: BehovRepository) : River.PacketListener {

    private val log = LoggerFactory.getLogger(this::class.java)
    private val sikkerLog = LoggerFactory.getLogger("tjenestekall")

    init {
        River(rapidsConnection).apply {
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

    override fun onError(problems: MessageProblems, context: MessageContext, metadata: MessageMetadata) {
        sikkerLog.error("forstår ikke behov:\n${problems.toExtendedReport()}")
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        val packetAsJson = objectMapper.readTree(packet.toJson()) as ObjectNode
        loggBehov(log, packetAsJson)
        loggBehov(sikkerLog, packetAsJson)

        val id = packetAsJson.behovId()
        val resultat = repository.hent(id)?.kombinerLøsninger(packetAsJson) ?: packetAsJson

        if (resultat.erKomplett()) {
            resultat.put("@final", true)
            resultat.put("@besvart", LocalDateTime.now().toString())
            loggLøstBehov(log, resultat)
            loggLøstBehov(sikkerLog, resultat)
            context.publish(objectMapper.writeValueAsString(resultat))
            repository.fjern(id)
        } else {
            repository.lagre(id, resultat)
        }
    }

    private fun JsonNode.erKomplett(): Boolean {
        val løsninger = this["@løsning"].feltnavn()
        val behov = this["@behov"].map(JsonNode::asText)
        return behov.all { it in løsninger }
    }

    private fun ObjectNode.kombinerLøsninger(packet: JsonNode): ObjectNode {
        val løsning = this["@løsning"] as ObjectNode
        packet["@løsning"].fields().forEach { (behovtype, delløsning) ->
            løsning.set<JsonNode>(behovtype, delløsning)
        }
        loggKombinering(log, this)
        loggKombinering(sikkerLog, this)
        return this
    }

    private fun loggLøstBehov(logger: Logger, løsning: JsonNode) {
        logger.info(
            "Markert behov {}, {} ({}) som final",
            keyValue("id", løsning["@id"].asText()),
            keyValue("behovId", løsning.behovId()),
            keyValue("vedtaksperiodeId", løsning["vedtaksperiodeId"]?.asText() ?: "IKKE_SATT")
        )
    }

    private fun loggKombinering(logger: Logger, løsningPacket: JsonNode) {
        val løsninger = løsningPacket["@løsning"].feltnavn()
        logger.info(
            "Satt sammen {} for behov {}, {} ({}). Status: {}, {}",
            keyValue("løsninger", løsninger.prettyPrint()),
            keyValue("id", løsningPacket["@id"].asText()),
            keyValue("behovId", løsningPacket.behovId()),
            keyValue("vedtaksperiodeId", løsningPacket["vedtaksperiodeId"]?.asText() ?: "IKKE_SATT"),
            keyValue("forespurte_behov", løsningPacket["@behov"].prettyPrint()),
            keyValue("manglende_behov", løsningPacket["@behov"].filter { it.asText() !in løsninger }.prettyPrint()),
        )
    }

    private fun loggBehov(logger: Logger, packet: JsonNode) {
        logger.info(
            "Mottok {} for behov {}, {} ({})",
            keyValue("løsninger", packet["@løsning"].feltnavn().prettyPrint()),
            keyValue("id", packet["@id"].asText()),
            keyValue("behovId", packet.behovId()),
            keyValue("vedtaksperiodeId", packet["vedtaksperiodeId"]?.asText() ?: "IKKE_SATT")
        )
    }

    private fun JsonNode.behovId() =
        this["@behovId"]?.asText() ?: this["@id"].asText().also {
            log.info("akkumulerer behov basert på gammel metode vha @id")
        }

    private fun JsonNode.feltnavn() = Iterable { fieldNames() }

    @Suppress("UNCHECKED_CAST")
    private inline fun <reified T> Iterable<T>.prettyPrint() =
        when (T::class) {
            JsonNode::class -> (this as Iterable<JsonNode>).map(JsonNode::asText)
            String::class -> this
            else -> throw UnsupportedOperationException()
        }.joinToString(prefix = "[", postfix = "]")
}
