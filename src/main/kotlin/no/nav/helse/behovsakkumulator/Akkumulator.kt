package no.nav.helse.behovsakkumulator

class Akkumulator {
    private val pågåendeBehandlinger : MutableMap<String, Innslag> = mutableMapOf()

    fun ubesvarteBehov() = pågåendeBehandlinger.keys.size
    fun behandle(behov: Behov) {
        pågåendeBehandlinger[behov.id]
            ?.let { pågåendeBehandlinger.put(behov.id, it.withDelløsning(behov)) }
            ?: pågåendeBehandlinger.put(behov.id, Innslag(orginalbehov = behov))
    }

    fun løsning(behovId: String) : Behov?{
        val muligKomplettLøsning = pågåendeBehandlinger[behovId]
            .takeIf { it?.delløsninger?.isNotEmpty() ?: false}
            ?.delløsninger?.reduce { acc, behov ->
                val løsning = behov.løsning?.entries?.first() ?: error("Fant løsning uten innhold")
                acc.løsning?.put(løsning.key, løsning.value)
                return@reduce acc
            }?.apply { oppdaterJsonNode() }

        return if (muligKomplettLøsning?.erKomplett() == true) { muligKomplettLøsning } else { null }
    }

    data class Innslag(
        val orginalbehov: Behov,
        val delløsninger: MutableList<Behov> = mutableListOf()
    ) {
        fun withDelløsning(delløsning: Behov) :Innslag{
            delløsninger.add(delløsning)
            return this
        }
    }
}