package no.nav.helse.behovsakkumulator

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

const val vaultBase = "/var/run/secrets/nais.io/serviceuser"
val vaultBasePath: Path = Paths.get(vaultBase)

fun readServiceUserCredentials() = ServiceUser(
    username = Files.readString(vaultBasePath.resolve("username")),
    password = Files.readString(vaultBasePath.resolve("password"))
)

fun setUpEnvironment() =
    Environment(
        kafkaBootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS")
            ?: error("Mangler env var KAFKA_BOOTSTRAP_SERVERS")
    )

data class Environment(
    val kafkaBootstrapServers: String,
    val spleisRapidtopic: String = "helse-rapid-v1"
)

data class ServiceUser(
    val username: String,
    val password: String
)
