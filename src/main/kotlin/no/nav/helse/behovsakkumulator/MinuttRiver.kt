package no.nav.helse.behovsakkumulator

import com.fasterxml.jackson.databind.JsonNode
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers.isMissingOrNull
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import java.time.LocalDateTime
import java.util.*
import net.logstash.logback.argument.StructuredArguments.keyValue
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class MinuttRiver(rapidsConnection: RapidsConnection, private val repository: BehovRepository) : River.PacketListener {
    private val log = LoggerFactory.getLogger(this::class.java)
    private val sikkerLog = LoggerFactory.getLogger("tjenestekall")

    init {
        River(rapidsConnection).apply {
            precondition {
                it.requireValue("@event_name", "minutt")
            }
        }.register(this)
    }

    override fun onError(problems: MessageProblems, context: MessageContext, metadata: MessageMetadata) {
        sikkerLog.error("forstår ikke minutt-melding:\n${problems.toExtendedReport()}")
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        fjernGamleBehovUtenSvar(context)
    }

    private fun fjernGamleBehovUtenSvar(context: MessageContext) {
        val grense = LocalDateTime.now().minusMinutes(30)
        repository.hentAlle()
            .filterValues { packet -> packet["@opprettet"].asLocalDateTime().isBefore(grense) }
            .forEach { (key, packet) ->
                val forventninger = packet["@behov"].map(JsonNode::asText)
                val løsninger = packet["@løsning"].feltnavn()
                val mangler = forventninger.filter { it !in løsninger }

                loggFjerneGammeltBehov(log, packet, mangler)
                loggFjerneGammeltBehov(sikkerLog, packet, mangler)
                repository.fjern(key)

                val behovId = packet.behovId()
                context.publish(
                    behovId, JsonMessage.newMessage(
                    mapOf(
                        "@event_name" to "behov_uten_fullstendig_løsning",
                        "@id" to UUID.randomUUID(),
                        "@opprettet" to LocalDateTime.now(),
                        "behov_id" to behovId,
                        "behov_opprettet" to packet["@opprettet"].asLocalDateTime(),
                        "forventet" to forventninger,
                        "løsninger" to løsninger,
                        "mangler" to mangler,
                        "ufullstendig_behov" to objectMapper.writeValueAsString(packet)
                    )
                ).toJson().also {
                    sikkerLog.info("sender event=behov_uten_fullstendig_løsning:\n\t$it")
                })
            }
    }

    private fun loggFjerneGammeltBehov(logger: Logger, packet: JsonNode, mangler: List<String>) {
        logger.warn(
            "Fjerner behov {}, {} for {}. Mottok aldri løsning(er) for {} innen 30 minutter.",
            keyValue("id", packet["@id"].asText()),
            keyValue("behovId", packet.behovId()),
            keyValue("vedtaksperiodeId", packet["vedtaksperiodeId"].asText("IKKE_SATT")),
            keyValue("manglende_behov", mangler.joinToString())
        )
    }

    private fun JsonNode.behovId() =
        this["@behovId"].takeUnless { it.isMissingOrNull() }?.asText() ?: this["@id"].asText().also {
            log.info("akkumulerer behov basert på gammel metode vha @id")
        }

    private fun JsonNode.feltnavn() = Iterable { fieldNames() }
}
