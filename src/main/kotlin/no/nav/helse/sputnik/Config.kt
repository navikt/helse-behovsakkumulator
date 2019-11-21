package no.nav.helse.sputnik

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

const val vaultBase = "/var/run/secrets/nais.io/vault"
val vaultBasePath: Path = Paths.get(vaultBase)

fun readServiceUserCredentials() = ServiceUser(
    username = Files.readString(vaultBasePath.resolve("username")),
    password = Files.readString(vaultBasePath.resolve("password"))
)

fun setUpEnvironment() =
    Environment(
        kafkaBootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS")
            ?: error("Mangler env var KAFKA_BOOTSTRAP_SERVERS"),
        fpsakBaseUrl = System.getenv("FPSAK_BASE_URL")
            ?: error("Mangler env var FPSAK_BASE_URL")
    )

data class Environment(
    val kafkaBootstrapServers: String,
    val spleisBehovtopic: String = "privat-helse-sykepenger-behov",
    val stsBaseUrl: String = "http://security-token-service",
    val fpsakBaseUrl: String
)

data class ServiceUser(
    val username: String,
    val password: String
)
