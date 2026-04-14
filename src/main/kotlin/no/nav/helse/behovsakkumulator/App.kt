package no.nav.helse.behovsakkumulator

import io.valkey.DefaultJedisClientConfig
import no.nav.helse.rapids_rivers.RapidApplication

fun main() {
    RapidApplication.create(System.getenv()).apply {
        val repository = ValkeyBehovRepository(
            host = System.getenv().getValue("VALKEY_HOST_BEHOVSAKKUMULATOR"),
            port = System.getenv().getValue("VALKEY_PORT_BEHOVSAKKUMULATOR").toInt(),
            jedisClientConfig = DefaultJedisClientConfig
                .builder()
                .ssl(true)
                .user(System.getenv().getValue("VALKEY_USERNAME_BEHOVSAKKUMULATOR"))
                .password(System.getenv().getValue("VALKEY_PASSWORD_BEHOVSAKKUMULATOR"))
                .build(),
        )
        Behovsakkumulator(this, repository)
        MinuttRiver(this, repository)
    }.start()
}
