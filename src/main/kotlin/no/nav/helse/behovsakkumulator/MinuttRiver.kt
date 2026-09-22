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
import no.nav.sykepenger.libs.logging.loggWarn
import no.nav.sykepenger.libs.logging.medMdc
import tools.jackson.databind.JsonNode
import java.time.LocalDateTime
import java.util.*

class MinuttRiver(
    rapidsConnection: RapidsConnection,
    private val repository: BehovRepository,
) : River.PacketListener {
    init {
        River(rapidsConnection)
            .apply {
                precondition {
                    it.requireValue("@event_name", "minutt")
                }
            }.register(this)
    }

    override fun onError(
        problems: MessageProblems,
        context: MessageContext,
        metadata: MessageMetadata,
    ) {
        loggError("Forstår ikke minutt-melding", "problemer" to problems.toExtendedReport())
    }

    override fun onPacket(
        packet: JsonMessage,
        context: MessageContext,
        metadata: MessageMetadata,
        meterRegistry: MeterRegistry,
    ) {
        fjernGamleBehovUtenSvar(context)
    }

    private fun fjernGamleBehovUtenSvar(context: MessageContext) {
        val grense = LocalDateTime.now().minusMinutes(30)
        repository
            .hentAlle()
            .filterValues { packet -> packet["@opprettet"].asLocalDateTime().isBefore(grense) }
            .forEach { (key, packet) ->
                val forventninger = packet["@behov"].toList().map(JsonNode::asString)
                val løsninger = packet["@løsning"].feltnavn()
                val mangler = forventninger.filter { it !in løsninger }
                val behovId = packet.behovId()

                medMdc(
                    MdcKey.MELDING_ID to packet["@id"]?.asString(),
                    MdcKey.VEDTAKSPERIODE_ID to
                        packet["vedtaksperiodeId"]
                            .takeUnless { it.isMissingOrNull() }
                            ?.asString(),
                ) {
                    loggWarn(
                        "Fjerner behov. Mottok aldri løsning(er) innen 30 minutter.",
                        "behovId" to behovId,
                        "manglende_behov" to mangler.joinToString(),
                    )
                    repository.fjern(key)

                    context.publish(
                        behovId,
                        JsonMessage
                            .newMessage(
                                mapOf(
                                    "@event_name" to "behov_uten_fullstendig_løsning",
                                    "@id" to UUID.randomUUID(),
                                    "@opprettet" to LocalDateTime.now(),
                                    "behov_id" to behovId,
                                    "behov_opprettet" to packet["@opprettet"].asLocalDateTime(),
                                    "forventet" to forventninger,
                                    "løsninger" to løsninger,
                                    "mangler" to mangler,
                                    "ufullstendig_behov" to objectMapper.writeValueAsString(packet),
                                ),
                            ).toJson()
                            .also {
                                loggInfo(
                                    "Sender event=behov_uten_fullstendig_løsning",
                                    "behovId" to behovId,
                                    "melding" to it,
                                )
                            },
                    )
                }
            }
    }

    private fun JsonNode.behovId() =
        this["@behovId"].takeUnless { it.isMissingOrNull() }?.asString() ?: this["@id"].asString().also {
            this@MinuttRiver.loggInfo("Akkumulerer behov basert på gammel metode vha @id")
        }

    private fun JsonNode.feltnavn() = propertyNames().asIterable()
}
