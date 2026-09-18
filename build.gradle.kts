plugins {
    alias(libs.plugins.sas.deployable)
}

sasDeployable {
    mainClass = "no.nav.helse.behovsakkumulator.AppKt"
}

dependencies {
    implementation(libs.rapidsAndRivers)
    implementation(libs.valkey)

    testImplementation(libs.rapidsAndRiversTest)
    testImplementation(libs.testcontainers)
    testRuntimeOnly(libs.junit.platform.launcher)
}
