plugins {
    alias(libs.plugins.sykepenger.deployable)
}

sykepengerDeployable {
    mainClass = "no.nav.helse.behovsakkumulator.AppKt"
}

dependencies {
    implementation(libs.rapidsAndRivers)
    implementation(libs.valkey)
    implementation(libs.logging)

    testImplementation(libs.rapidsAndRiversTest)
    testImplementation(libs.testcontainers)
    testRuntimeOnly(libs.junit.platform.launcher)
}
