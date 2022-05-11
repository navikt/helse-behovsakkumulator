package no.nav.helse.behovsakkumulator

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.rapids_rivers.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.util.*

class Behovsakkumulator(rapidsConnection: RapidsConnection) : River.PacketListener {

    private val log = LoggerFactory.getLogger(this::class.java)
    private val sikkerLog = LoggerFactory.getLogger("tjenestekall")

    private val behovUtenLøsning = mutableMapOf<String, JsonMessage>()

    init {
        River(rapidsConnection).apply {
            validate {
                it.demandKey("@behov")
                it.demandKey("@løsning")
                it.rejectKey("@final")
                it.requireKey("@id")
                it.interestedIn("@behovId")
                it.interestedIn("vedtaksperiodeId")
                it.require("@opprettet", JsonNode::asLocalDateTime)
            }
        }.register(this)
    }

    override fun onError(problems: MessageProblems, context: MessageContext) {
        sikkerLog.error("forstår ikke behov:\n${problems.toExtendedReport()}")
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        loggBehov(log, packet)
        loggBehov(sikkerLog, packet)

        val id = packet.behovId()
        val resultat = behovUtenLøsning[id]?.kombinerLøsninger(packet) ?: packet

        if (resultat.erKomplett()) {
            resultat["@final"] = true
            resultat["@besvart"] = LocalDateTime.now().toString()
            loggLøstBehov(log, resultat)
            loggLøstBehov(sikkerLog, resultat)
            context.publish(resultat.toJson())
            behovUtenLøsning.remove(id)
        } else {
            fjernGamleBehovUtenSvar(context)
            behovUtenLøsning[id] = resultat
        }
    }

    private fun fjernGamleBehovUtenSvar(context: MessageContext) {
        val grense = LocalDateTime.now().minusMinutes(30)
        behovUtenLøsning
            .filterValues { packet -> packet["@opprettet"].asLocalDateTime().isBefore(grense) }
            .forEach { (key, packet) ->
                val forventninger = packet["@behov"].map(JsonNode::asText)
                val løsninger = packet["@løsning"].feltnavn()
                val mangler = forventninger.filter { it !in løsninger }

                loggFjerneGammeltBehov(log, packet, mangler)
                loggFjerneGammeltBehov(sikkerLog, packet, mangler)
                behovUtenLøsning.remove(key)

                val behovId = packet.behovId()
                context.publish(behovId, JsonMessage.newMessage(mapOf(
                    "@event_name" to "behov_uten_fullstendig_løsning",
                    "@id" to UUID.randomUUID(),
                    "@opprettet" to LocalDateTime.now(),
                    "behov_id" to behovId,
                    "behov_opprettet" to packet["@opprettet"].asLocalDateTime(),
                    "forventet" to forventninger,
                    "løsninger" to løsninger,
                    "mangler" to mangler,
                    "ufullstendig_behov" to packet.toJson()
                )).toJson().also {
                    sikkerLog.info("sender event=behov_uten_fullstendig_løsning:\n\t$it")
                })
            }
    }

    private fun JsonMessage.erKomplett(): Boolean {
        val løsninger = this["@løsning"].feltnavn()
        val behov = this["@behov"].map(JsonNode::asText)
        return behov.all { it in løsninger }
    }

    private fun JsonMessage.kombinerLøsninger(packet: JsonMessage): JsonMessage {
        val løsning = this["@løsning"] as ObjectNode
        packet["@løsning"].fields().forEach { (behovtype, delløsning) ->
            løsning.set<JsonNode>(behovtype, delløsning)
        }
        loggKombinering(log, this)
        loggKombinering(sikkerLog, this)
        return this
    }

    private fun loggLøstBehov(logger: Logger, løsning: JsonMessage) {
        logger.info(
            "Markert behov {}, {} ({}) som final",
            keyValue("id", løsning["@id"].asText()),
            keyValue("behovId", løsning.behovId()),
            keyValue("vedtaksperiodeId", løsning["vedtaksperiodeId"].asText("IKKE_SATT"))
        )
    }

    private fun loggKombinering(logger: Logger, løsningPacket: JsonMessage) {
        val løsninger = løsningPacket["@løsning"].feltnavn()
        logger.info(
            "Satt sammen {} for behov {}, {} ({}). Status: {}, {}",
            keyValue("løsninger", løsninger.prettyPrint()),
            keyValue("id", løsningPacket["@id"].asText()),
            keyValue("behovId", løsningPacket.behovId()),
            keyValue("vedtaksperiodeId", løsningPacket["vedtaksperiodeId"].asText("IKKE_SATT")),
            keyValue("forespurte_behov", løsningPacket["@behov"].prettyPrint()),
            keyValue("manglende_behov", løsningPacket["@behov"].filter { it.asText() !in løsninger }.prettyPrint()),
        )
    }

    private fun loggBehov(logger: Logger, packet: JsonMessage) {
        logger.info(
            "Mottok {} for behov {}, {} ({})",
            keyValue("løsninger", packet["@løsning"].feltnavn().prettyPrint()),
            keyValue("id", packet["@id"].asText()),
            keyValue("behovId", packet.behovId()),
            keyValue("vedtaksperiodeId", packet["vedtaksperiodeId"].asText("IKKE_SATT"))
        )
    }

    private fun loggFjerneGammeltBehov(logger: Logger, packet: JsonMessage, mangler: List<String>) {
        logger.warn(
            "Fjerner behov {}, {} for {}. Mottok aldri løsning(er) for {} innen 30 minutter.",
            keyValue("id", packet["@id"].asText()),
            keyValue("behovId", packet.behovId()),
            keyValue("vedtaksperiodeId", packet["vedtaksperiodeId"].asText("IKKE_SATT")),
            keyValue("manglende_behov", mangler.joinToString())
        )
    }

    private fun JsonMessage.behovId() =
        this["@behovId"].takeUnless { it.isMissingOrNull() }?.asText() ?: this["@id"].asText().also {
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
