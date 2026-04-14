package no.nav.helse.behovsakkumulator

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.valkey.HostAndPort
import io.valkey.JedisClientConfig
import io.valkey.JedisPooled
import io.valkey.params.ScanParams

interface BehovRepository {
    fun hent(id: String): ObjectNode?
    fun lagre(id: String, melding: JsonNode)
    fun fjern(id: String)
    fun hentAlle(): Map<String, JsonNode>
}

class ValkeyBehovRepository(
    host: String,
    port: Int,
    jedisClientConfig: JedisClientConfig,
) : BehovRepository {

    private companion object {
        const val NØKKEL_PREFIKS = "behov:"
        const val TTL_SEKUNDER = 60L * 60L // 60 minutter
    }

    private val jedis = JedisPooled(
        HostAndPort(host, port),
        jedisClientConfig,
    )

    private fun nøkkel(id: String) = "$NØKKEL_PREFIKS$id"

    override fun hent(id: String): ObjectNode? =
        jedis.get(nøkkel(id))?.tilObjectNode()

    override fun lagre(id: String, melding: JsonNode) {
        jedis.setex(nøkkel(id), TTL_SEKUNDER, objectMapper.writeValueAsString(melding))
    }

    override fun fjern(id: String) {
        jedis.del(nøkkel(id))
    }

    override fun hentAlle(): Map<String, JsonNode> {
        val resultat = mutableMapOf<String, JsonNode>()
        val scanParams = ScanParams().match("$NØKKEL_PREFIKS*").count(100)
        var cursor = ScanParams.SCAN_POINTER_START
        do {
            val scanResult = jedis.scan(cursor, scanParams)
            scanResult.result.forEach { nøkkel ->
                jedis.get(nøkkel)?.let { json ->
                    resultat[nøkkel.removePrefix(NØKKEL_PREFIKS)] = json.tilObjectNode()
                }
            }
            cursor = scanResult.cursor
        } while (cursor != ScanParams.SCAN_POINTER_START)
        return resultat
    }

    private fun String.tilObjectNode() = objectMapper.readTree(this) as ObjectNode
}
