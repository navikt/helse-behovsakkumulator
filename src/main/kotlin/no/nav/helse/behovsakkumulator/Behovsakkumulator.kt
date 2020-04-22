package no.nav.helse.behovsakkumulator

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.rapids_rivers.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDateTime

class Behovsakkumulator(rapidsConnection: RapidsConnection) : River.PacketListener {

    private val log = LoggerFactory.getLogger(this::class.java)
    private val sikkerLog = LoggerFactory.getLogger("tjenestekall")

    private val behovUtenLøsning = mutableMapOf<String, Pair<RapidsConnection.MessageContext, JsonMessage>>()

    init {
        River(rapidsConnection).apply {
            validate {
                it.demandKey("@behov")
                it.demandKey("@løsning")
                it.rejectKey("@final")
                it.requireKey("@id", "vedtaksperiodeId")
                it.require("@opprettet", JsonNode::asLocalDateTime)
            }
        }.register(this)
    }

    override fun onError(problems: MessageProblems, context: RapidsConnection.MessageContext) {
        sikkerLog.error("forstår ikke behov:\n${problems.toExtendedReport()}")
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
            fjernGamleBehovUtenSvar()
            behovUtenLøsning[id] = resultat
        }
    }

    private fun fjernGamleBehovUtenSvar() {
        val grense = LocalDateTime.now().minusMinutes(30)
        behovUtenLøsning
            .filterValues { (_, packet) -> packet["@opprettet"].asLocalDateTime().isBefore(grense) }
            .forEach { (key, value) ->
                loggFjerneGammeltBehov(log, value.second)
                loggFjerneGammeltBehov(sikkerLog, value.second)
                behovUtenLøsning.remove(key)
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
        val løsninger = løsningPacket["@løsning"].fieldNames().asSequence().toList()
        logger.info(
            "Satt sammen {} for behov med id {} ({}). Mangler {}. Forventer {}",
            keyValue("løsninger", løsninger.joinToString(", ")),
            keyValue("id", løsningPacket["@id"].asText()),
            keyValue("vedtaksperiodeId", løsningPacket["vedtaksperiodeId"].asText()),
            keyValue("mangler_behov", løsningPacket["@behov"].filter { it.asText() !in løsninger }.joinToString(", ", transform = JsonNode::asText)),
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

    private fun loggFjerneGammeltBehov(logger: Logger, packet: JsonMessage) {
        val forventninger = packet["@behov"].map(JsonNode::asText)
        val løsninger = packet["@løsning"].fieldNames().asSequence().toList()
        val mangler = forventninger.filter { it !in løsninger }
        logger.error(
            "Fjerner behov {} for {}. Mottok aldri løsning for {} innen 30 minutter.",
            keyValue("id", packet["@id"].asText()),
            keyValue("vedtaksperiodeId", packet["vedtaksperiodeId"].asText()),
            keyValue("behov", mangler.joinToString(", "))
        )
    }
}
