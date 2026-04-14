package no.nav.helse.behovsakkumulator

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

class ValkeyContainer : GenericContainer<ValkeyContainer?>(DockerImageName.parse("valkey/valkey:9")) {
    init {
        withExposedPorts(VALKEY_PORT)
        withEnv("VALKEY_EXTRA_FLAGS", "--maxmemory 2mb --maxmemory-policy allkeys-lru")
        setWaitStrategy(Wait.forListeningPort())
    }

    fun getPort(): Int = getMappedPort(VALKEY_PORT)

    companion object {
        private const val VALKEY_PORT = 6379
    }
}
