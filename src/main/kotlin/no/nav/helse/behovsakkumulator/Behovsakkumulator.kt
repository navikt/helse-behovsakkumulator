package no.nav.helse.behovsakkumulator

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageProblems
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDateTime

class Behovsakkumulator(rapidsConnection: RapidsConnection) : River.PacketListener {

    private val log = LoggerFactory.getLogger(this::class.java)
    private val sikkerLog = LoggerFactory.getLogger("tjenestekall")

    private val behovUtenLøsning = mutableMapOf<String, Pair<RapidsConnection.MessageContext, JsonMessage>>()

    init {
        River(rapidsConnection).apply {
            validate { it.forbid("@final") }
            validate { it.requireKey("@id", "@behov", "@løsning", "vedtaksperiodeId") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        loggBehov(log, packet)
        loggBehov(sikkerLog, packet)

        val id = packet["@id"].asText()
        val resultat = behovUtenLøsning[id]?.also { it.second.kombinerLøsninger(packet) } ?: (context to packet)

        if (resultat.second.erKomplett()) {
            resultat.second["@final"] = true
            resultat.second["@besvart"] = LocalDateTime.now().toString()
            loggLøstBehov(log, resultat.second)
            loggLøstBehov(sikkerLog, resultat.second)
            resultat.first.send(resultat.second.toJson())
            behovUtenLøsning.remove(id)
        } else {
            behovUtenLøsning[id] = resultat
        }
    }

    private fun JsonMessage.erKomplett(): Boolean {
        val løsninger = this["@løsning"].fieldNames().asSequence().toList()
        val behov = this["@behov"].map(JsonNode::asText)
        return behov.all { it in løsninger }
    }

    private fun JsonMessage.kombinerLøsninger(packet: JsonMessage) {
        val løsning = this["@løsning"] as ObjectNode
        packet["@løsning"].fields().forEach { (behovtype, delløsning) ->
            løsning.set<JsonNode>(behovtype, delløsning)
        }
        loggKombinering(log, this)
        loggKombinering(sikkerLog, this)
    }

    private fun loggLøstBehov(logger: Logger, løsning: JsonMessage) {
        logger.info(
            "Markert behov med {} ({}) som final",
            keyValue("id", løsning["@id"].asText()),
            keyValue("vedtaksperiodeId", løsning["vedtaksperiodeId"].asText())
        )
    }

    private fun loggKombinering(logger: Logger, løsningPacket: JsonMessage) {
        logger.info(
            "Satt sammen {} for behov med id {} ({}). Forventer {}",
            keyValue("løsninger", løsningPacket["@løsning"].fieldNames().asSequence().joinToString(", ")),
            keyValue("id", løsningPacket["@id"].asText()),
            keyValue("vedtaksperiodeId", løsningPacket["vedtaksperiodeId"].asText()),
            keyValue("behov", løsningPacket["@behov"].joinToString(", ", transform = JsonNode::asText))
        )
    }

    private fun loggBehov(logger: Logger, packet: JsonMessage) {
        logger.info(
            "Mottok {} for behov med {} ({})",
            keyValue("løsninger", packet["@løsning"].fieldNames().asSequence().joinToString(", ")),
            keyValue("id", packet["@id"].asText()),
            keyValue("vedtaksperiodeId", packet["vedtaksperiodeId"].asText())
        )
    }
}
